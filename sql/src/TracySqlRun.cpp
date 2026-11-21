// Statement splitting, execution and result formatting.

#include <charconv>
#include <chrono>
#include <deque>
#include <string>
#include <vector>

#include <ctype.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

#include <sqlite3.h>

#include "TracySql.hpp"

namespace tracysql
{

static const int HeadTailRows = 20;

// Skips whitespace, comments and empty statements.
static const char* SkipBlank( const char* p )
{
    for(;;)
    {
        while( *p && ( isspace( (unsigned char)*p ) || *p == ';' ) ) p++;
        if( p[0] == '-' && p[1] == '-' )
        {
            while( *p && *p != '\n' ) p++;
        }
        else if( p[0] == '/' && p[1] == '*' )
        {
            p += 2;
            while( *p && !( p[0] == '*' && p[1] == '/' ) ) p++;
            if( *p ) p += 2;
        }
        else
        {
            return p;
        }
    }
}

// Returns the position just after the statement starting at p (after its ';' if any).
static const char* StatementEnd( const char* p )
{
    while( *p )
    {
        const char c = *p;
        if( c == ';' ) return p + 1;
        if( c == '\'' || c == '"' || c == '`' )
        {
            p++;
            while( *p )
            {
                if( *p == c )
                {
                    if( p[1] == c ) p++;
                    else break;
                }
                p++;
            }
            if( *p ) p++;
        }
        else if( c == '[' )
        {
            while( *p && *p != ']' ) p++;
            if( *p ) p++;
        }
        else if( c == '-' && p[1] == '-' )
        {
            while( *p && *p != '\n' ) p++;
        }
        else if( c == '/' && p[1] == '*' )
        {
            p += 2;
            while( *p && !( p[0] == '*' && p[1] == '/' ) ) p++;
            if( *p ) p += 2;
        }
        else
        {
            p++;
        }
    }
    return p;
}

// Matches a sequence of keywords, case-insensitively, separated by whitespace. Returns the position after them.
static const char* MatchWords( const char* p, std::initializer_list<const char*> words )
{
    for( auto w : words )
    {
        while( isspace( (unsigned char)*p ) ) p++;
        const size_t n = strlen( w );
        if( strncasecmp( p, w, n ) != 0 ) return nullptr;
        if( isalnum( (unsigned char)p[n] ) || p[n] == '_' ) return nullptr;
        p += n;
    }
    return p;
}

static std::string JsonString( const std::string& s )
{
    std::string r = "\"";
    for( unsigned char c : s )
    {
        switch( c )
        {
        case '"': r += "\\\""; break;
        case '\\': r += "\\\\"; break;
        case '\n': r += "\\n"; break;
        case '\r': r += "\\r"; break;
        case '\t': r += "\\t"; break;
        default:
            if( c < 0x20 )
            {
                char buf[8];
                snprintf( buf, sizeof( buf ), "\\u%04x", c );
                r += buf;
            }
            else
            {
                r.push_back( char( c ) );
            }
        }
    }
    r.push_back( '"' );
    return r;
}

static std::string FormatDouble( double d )
{
    if( isnan( d ) ) return "nan";
    if( isinf( d ) ) return d > 0 ? "inf" : "-inf";
    char buf[64];
    auto res = std::to_chars( buf, buf + sizeof( buf ), d );
    std::string s( buf, res.ptr );
    if( s.find_first_of( ".e" ) == std::string::npos ) s += ".0";
    return s;
}

// Cuts s to at most maxChars UTF-8 characters.
static void CutCell( std::string& s, int64_t maxChars )
{
    if( maxChars <= 0 || int64_t( s.size() ) <= maxChars ) return;
    int64_t chars = 0;
    size_t i = 0;
    while( i < s.size() )
    {
        if( ( (unsigned char)s[i] & 0xC0 ) != 0x80 )
        {
            if( chars == maxChars ) break;
            chars++;
        }
        i++;
    }
    if( i < s.size() )
    {
        s.resize( i );
        s += "...";
    }
}

struct Printer
{
    const OutputOptions& o;
    Sink& sink;
    std::string buf;

    void Flush()
    {
        if( !buf.empty() ) sink.Out( buf.data(), buf.size() );
        buf.clear();
    }
    void Put( const std::string& s )
    {
        buf += s;
        if( buf.size() > 64 * 1024 ) Flush();
    }
};

static std::string CellText( sqlite3_stmt* st, int i, const OutputOptions& o )
{
    const bool md = o.format == OutputOptions::Format::Markdown;
    std::string s;
    switch( sqlite3_column_type( st, i ) )
    {
    case SQLITE_NULL:
        return md ? "NULL" : "\"[NULL]\"";
    case SQLITE_INTEGER:
        return std::to_string( sqlite3_column_int64( st, i ) );
    case SQLITE_FLOAT:
        return FormatDouble( sqlite3_column_double( st, i ) );
    case SQLITE_BLOB:
        s = "<blob " + std::to_string( sqlite3_column_bytes( st, i ) ) + " bytes>";
        break;
    default:
    {
        const char* t = (const char*)sqlite3_column_text( st, i );
        s.assign( t ? t : "", size_t( sqlite3_column_bytes( st, i ) ) );
        break;
    }
    }
    CutCell( s, o.maxCellWidth );
    std::string r;
    if( md )
    {
        for( char c : s )
        {
            if( c == '|' ) r += "\\|";
            else if( c == '\n' ) r += "\\n";
            else if( c == '\r' ) continue;
            else r.push_back( c );
        }
    }
    else
    {
        r.push_back( '"' );
        for( char c : s )
        {
            if( c == '"' ) r += "\"\"";
            else r.push_back( c );
        }
        r.push_back( '"' );
    }
    return r;
}

static std::string RowText( sqlite3_stmt* st, int cols, const OutputOptions& o )
{
    std::string r;
    if( o.format == OutputOptions::Format::Markdown )
    {
        r = "|";
        for( int i = 0; i < cols; i++ )
        {
            r += " ";
            r += CellText( st, i, o );
            r += " |";
        }
    }
    else
    {
        for( int i = 0; i < cols; i++ )
        {
            if( i ) r += ",";
            r += CellText( st, i, o );
        }
    }
    r += "\n";
    return r;
}

// Prints one result set. Returns the sqlite result code of the last step.
static int PrintResult( sqlite3_stmt* st, Printer& pr, std::string& note )
{
    const auto& o = pr.o;
    const bool md = o.format == OutputOptions::Format::Markdown;
    const int cols = sqlite3_column_count( st );

    std::string header;
    if( md )
    {
        header = "|";
        for( int i = 0; i < cols; i++ )
        {
            std::string n = sqlite3_column_name( st, i );
            for( auto& c : n ) if( c == '|' || c == '\n' ) c = ' ';
            header += " " + n + " |";
        }
        header += "\n|";
        for( int i = 0; i < cols; i++ ) header += "---|";
        header += "\n";
    }
    else
    {
        for( int i = 0; i < cols; i++ )
        {
            if( i ) header += ",";
            std::string n = sqlite3_column_name( st, i );
            header += "\"";
            for( char c : n ) { if( c == '"' ) header += "\"\""; else header.push_back( c ); }
            header += "\"";
        }
        header += "\n";
    }

    const bool capped = o.maxRows > 0 || o.maxBytes > 0;
    if( !capped )
    {
        pr.Put( header );
        int rc;
        while( ( rc = sqlite3_step( st ) ) == SQLITE_ROW ) pr.Put( RowText( st, cols, o ) );
        return rc;
    }

    // Buffer rows until a cap is exceeded; from then on only keep what the cut output needs.
    std::vector<std::string> rows;
    std::deque<std::string> tail;
    int64_t count = 0, bytes = 0;
    bool over = false;
    int rc;
    while( ( rc = sqlite3_step( st ) ) == SQLITE_ROW )
    {
        count++;
        if( over && !md ) continue;     // csv keeps only the first rows, but the total is still reported
        std::string row = RowText( st, cols, o );
        if( !over )
        {
            bytes += int64_t( row.size() );
            rows.emplace_back( std::move( row ) );
            if( ( o.maxRows > 0 && count > o.maxRows ) || ( o.maxBytes > 0 && bytes > o.maxBytes ) )
            {
                over = true;
                if( md )
                {
                    // rows[] becomes the head, the last HeadTailRows of the rest the tail
                    const size_t head = std::min<size_t>( rows.size(), HeadTailRows );
                    for( size_t i = head; i < rows.size(); i++ ) tail.emplace_back( std::move( rows[i] ) );
                    rows.resize( head );
                    while( tail.size() > size_t( HeadTailRows ) ) tail.pop_front();
                }
                else
                {
                    rows.pop_back();
                }
            }
        }
        else
        {
            tail.emplace_back( std::move( row ) );
            if( tail.size() > size_t( HeadTailRows ) ) tail.pop_front();
        }
    }
    if( rc != SQLITE_DONE ) return rc;

    pr.Put( header );
    for( auto& r : rows ) pr.Put( r );
    if( over )
    {
        if( md )
        {
            const int64_t shown = int64_t( rows.size() + tail.size() );
            if( shown < count ) pr.Put( "| ... |\n" );
            for( auto& r : tail ) pr.Put( r );
            pr.Put( "\n(first " + std::to_string( rows.size() ) + " and last " + std::to_string( tail.size() ) + " of " + std::to_string( count ) +
                    " rows. Page with LIMIT/OFFSET, or pass --max-rows 0 --max-bytes 0 for all)\n" );
        }
        else
        {
            note = "note: result cut to its first " + std::to_string( rows.size() ) + " of " + std::to_string( count ) + " rows by --max-rows/--max-bytes\n";
        }
    }
    return rc;
}

static void LineCol( const std::string& sql, size_t off, int& line, int& col )
{
    line = 1;
    col = 1;
    for( size_t i = 0; i < off && i < sql.size(); i++ )
    {
        if( sql[i] == '\n' ) { line++; col = 1; }
        else col++;
    }
}

static void ReportError( const std::string& msg, int stmtNo, const std::string& sql, size_t off, bool haveOff, const OutputOptions& o, Sink& sink )
{
    int line, col;
    LineCol( sql, off, line, col );
    // source line
    size_t ls = off;
    while( ls > 0 && sql[ls-1] != '\n' ) ls--;
    size_t le = off;
    while( le < sql.size() && sql[le] != '\n' ) le++;
    const std::string src = sql.substr( ls, le - ls );
    const std::string num = std::to_string( line );
    const std::string pad( num.size(), ' ' );
    std::string tb = " --> query:" + num + ":" + std::to_string( col ) + "\n" + pad + " |\n" + num + " | " + src + "\n" + pad + " | " + std::string( size_t( col - 1 ), ' ' ) + "^\n";

    std::string out;
    if( o.agent )
    {
        std::string hint;
        if( msg.find( "no such column" ) != std::string::npos ) hint = "`SELECT * FROM <table> LIMIT 0` prints a table's columns";
        else if( msg.find( "no such table" ) != std::string::npos ) hint = "`SELECT name FROM perfetto_tables` lists every table and view";
        else if( msg.find( "no such function" ) != std::string::npos ) hint = "this is SQLite, not PerfettoSQL: only trace_start(), trace_end(), trace_dur() and regexp() are added; see `tracy-sql help agent`";
        else if( msg.find( "syntax error" ) != std::string::npos ) hint = "this is SQLite, not PerfettoSQL: CREATE PERFETTO FUNCTION/MACRO and stdlib modules other than slices.with_context are not available";
        out = "{\"error\":" + JsonString( msg ) + ",\"statement\":" + std::to_string( stmtNo );
        if( haveOff ) out += ",\"location\":" + JsonString( "query:" + num + ":" + std::to_string( col ) ) + ",\"traceback\":" + JsonString( tb );
        if( !hint.empty() ) out += ",\"hint\":" + JsonString( hint );
        out += "}\n";
    }
    else
    {
        out = "Error in statement " + std::to_string( stmtNo ) + ": " + msg + "\n";
        if( haveOff ) out += tb;
    }
    sink.Err( out.data(), out.size() );
}

int RunSql( sqlite3* db, const std::string& sql, const OutputOptions& opts, Sink& sink )
{
    Printer pr { opts, sink };
    const auto t0 = std::chrono::steady_clock::now();
    const char* p = sql.c_str();
    int stmtNo = 0;
    int results = 0;
    for(;;)
    {
        p = SkipBlank( p );
        if( !*p ) break;
        stmtNo++;
        const char* end = StatementEnd( p );
        const size_t stmtOff = size_t( p - sql.c_str() );

        // PerfettoSQL compatibility: modules are built in, and PERFETTO tables/views are plain ones.
        if( MatchWords( p, { "INCLUDE", "PERFETTO", "MODULE" } ) )
        {
            p = end;
            continue;
        }
        std::string text( p, end );
        std::string pre;
        size_t shift = 0;   // how far the rewritten text moved to the left
        {
            const char* s = text.c_str();
            const char* afterCreate = MatchWords( s, { "CREATE" } );
            if( afterCreate )
            {
                const char* a = afterCreate;
                bool replace = false;
                if( const char* r = MatchWords( a, { "OR", "REPLACE" } ) ) { a = r; replace = true; }
                if( const char* q = MatchWords( a, { "PERFETTO" } ) )
                {
                    const char* kind = nullptr;
                    const char* k;
                    if( ( k = MatchWords( q, { "TABLE" } ) ) ) kind = "TABLE";
                    else if( ( k = MatchWords( q, { "VIEW" } ) ) ) kind = "VIEW";
                    if( kind )
                    {
                        while( isspace( (unsigned char)*k ) ) k++;
                        const char* ne = k;
                        while( isalnum( (unsigned char)*ne ) || *ne == '_' ) ne++;
                        if( replace ) pre = std::string( "DROP " ) + kind + " IF EXISTS " + std::string( k, ne );
                        const std::string rewritten = std::string( "CREATE " ) + kind + " " + std::string( k );
                        shift = text.size() - rewritten.size();
                        text = rewritten;
                    }
                }
            }
        }
        p = end;

        if( !pre.empty() )
        {
            char* msg = nullptr;
            if( sqlite3_exec( db, pre.c_str(), nullptr, nullptr, &msg ) != SQLITE_OK )
            {
                pr.Flush();
                ReportError( msg ? msg : "error", stmtNo, sql, stmtOff, false, opts, sink );
                sqlite3_free( msg );
                return 1;
            }
        }

        sqlite3_stmt* st = nullptr;
        int rc = sqlite3_prepare_v2( db, text.c_str(), int( text.size() ), &st, nullptr );
        if( rc != SQLITE_OK )
        {
            const int eo = sqlite3_error_offset( db );
            pr.Flush();
            ReportError( sqlite3_errmsg( db ), stmtNo, sql, stmtOff + ( eo >= 0 ? size_t( eo ) + ( size_t( eo ) > 6 ? shift : 0 ) : 0 ), true, opts, sink );
            sqlite3_finalize( st );
            return 1;
        }
        if( !st ) continue;

        std::string note;
        if( sqlite3_column_count( st ) > 0 )
        {
            if( results++ > 0 ) pr.Put( "\n" );
            rc = PrintResult( st, pr, note );
        }
        else
        {
            while( ( rc = sqlite3_step( st ) ) == SQLITE_ROW ) {}
        }
        if( rc != SQLITE_DONE )
        {
            const std::string msg = sqlite3_errmsg( db );
            sqlite3_finalize( st );
            pr.Flush();
            ReportError( msg, stmtNo, sql, stmtOff, true, opts, sink );
            return 1;
        }
        sqlite3_finalize( st );
        pr.Flush();
        if( !note.empty() ) sink.Err( note.data(), note.size() );
    }
    pr.Flush();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>( std::chrono::steady_clock::now() - t0 ).count();
    const std::string timing = "Query executed in " + std::to_string( ms ) + " ms\n";
    sink.Err( timing.data(), timing.size() );
    return 0;
}

}
