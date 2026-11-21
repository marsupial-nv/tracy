// tracy-sql: run SQL against a Tracy capture. The command line follows
// Perfetto's trace_processor (query / server unix / query --remote / server kill).

#include <chrono>
#include <string>
#include <vector>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "../../public/common/TracyVersion.hpp"
#include "GitRef.hpp"
#include "TracySql.hpp"

using namespace tracysql;

static const char* Usage =
    "tracy-sql - SQL over Tracy captures\n"
    "\n"
    "Usage:\n"
    "  tracy-sql query [FLAGS] TRACE.tracy [SQL]        load a trace and run SQL (one-shot)\n"
    "  tracy-sql query [FLAGS] --remote NAME [SQL]      run SQL in a warm session\n"
    "  tracy-sql server unix [--name NAME] [--daemonize] [--idle-timeout T] TRACE.tracy\n"
    "  tracy-sql server kill NAME                       stop a session\n"
    "  tracy-sql server list                            list running sessions\n"
    "  tracy-sql help [agent]                           this text / guide for scripts and AI agents\n"
    "\n"
    "SQL comes from the positional argument, from -f FILE, or from stdin. Several\n"
    "';'-separated statements are fine; each result set is printed in turn,\n"
    "separated by a blank line.\n"
    "\n"
    "query flags:\n"
    "  -f, --query-file FILE   read SQL from FILE ('-' for stdin)\n"
    "      --remote NAME       use the session started by 'server unix --name NAME'\n"
    "      --format FMT        csv (default) or markdown\n"
    "      --agent, --no-agent force agent mode on or off. Agent mode (markdown tables, capped\n"
    "                          results, one-line JSON errors) is on by default when CLAUDECODE\n"
    "                          or AI_AGENT is set, stdout is not a terminal and no --format is given\n"
    "      --max-rows N        rows printed in full; larger results are cut (markdown default 1000, 0 = no limit)\n"
    "      --max-bytes N       like --max-rows, for bytes of output (markdown default 10000)\n"
    "      --max-cell-width N  characters printed per cell (markdown default 500)\n"
    "      --quiet             no status lines on stderr\n"
    "\n"
    "server flags:\n"
    "      --name NAME         session name (default: tracy)\n"
    "      --path PATH         explicit socket path instead of a name\n"
    "      --daemonize         detach into the background once the trace is loaded\n"
    "      --idle-timeout T    exit after this much inactivity, e.g. 30m, 90s, 2h (default 30m; 0 = never)\n";

static const char* AgentHelp =
    "tracy-sql for scripts and AI agents\n"
    "===================================\n"
    "\n"
    "1. Load the trace once, then run many queries against it.\n"
    "\n"
    "     tracy-sql server unix --name SESSION --daemonize TRACE.tracy\n"
    "     tracy-sql query --remote SESSION \"SELECT ts, dur, name FROM slice LIMIT 5\"\n"
    "     tracy-sql query --remote SESSION -f queries.sql        # long SQL from a file\n"
    "     tracy-sql server kill SESSION                          # when completely done\n"
    "\n"
    "   Tables and views created in one call stay available for the next. Several\n"
    "   ';'-separated statements per call are fine; each result set is printed in\n"
    "   turn. One-shot form (loads the trace every time): tracy-sql query TRACE.tracy \"SELECT ...\"\n"
    "\n"
    "2. Reading output. Run by a coding agent (CLAUDECODE or AI_AGENT set, stdout not a\n"
    "   terminal), `query` is in agent mode:\n"
    "   - Results are compact markdown tables.\n"
    "   - Results over 1000 rows or 10KB show their first and last 20 rows and a\n"
    "     footer like `(first 20 and last 20 of 5000 rows. ...)`. Aggregate or LIMIT\n"
    "     rather than raising the caps (--max-rows/--max-bytes, 0 = no limit).\n"
    "   - Cells are cut at 500 characters (--max-cell-width).\n"
    "   - Errors are one JSON line on stderr with the error, the failing statement,\n"
    "     its location (query:LINE:COL) and often a hint. Exit code is 1.\n"
    "   --agent / --no-agent force it on or off; --format csv gives plain CSV.\n"
    "\n"
    "3. Discover instead of guessing.\n"
    "\n"
    "     SELECT name FROM perfetto_tables;      -- every table and view\n"
    "     SELECT * FROM slice LIMIT 0;           -- exact columns of any table, view or query\n"
    "     SELECT * FROM metadata;                -- program, host, time range, counts\n"
    "\n"
    "4. Tables. Names and columns follow Perfetto's trace_processor where the data\n"
    "   matches, so the same queries work on both. All times are nanoseconds.\n"
    "\n"
    "   Perfetto-compatible\n"
    "     slice          Tracy CPU zones: id, ts, dur, track_id, category (NULL), name, depth,\n"
    "                    parent_id, + utid, source_location_id, file, line, function,\n"
    "                    zone_name (per-zone dynamic name), text (zone text), color\n"
    "     thread_slice   slice + thread_name, tid, is_main_thread, process_name, pid\n"
    "                    (Perfetto's slices.with_context; INCLUDE PERFETTO MODULE is accepted and ignored)\n"
    "     thread         utid, tid, name, start_ts, end_ts, is_main_thread, + is_fiber, zone_count\n"
    "     process        one row: upid 0, pid, name\n"
    "     thread_track   id (== utid), utid\n"
    "     counter_track  Tracy plots: id, name, unit, + plot_type, format, min, max, count\n"
    "     counter        plot points: id, ts, track_id, value\n"
    "     track          thread_track + counter_track\n"
    "     metadata       name, key_type, int_value, str_value\n"
    "   Tracy only\n"
    "     source_location  id, name, function, file, line, color, zone_count\n"
    "     frame            id, frame_set_id, name (frame set), idx, ts, dur, continuous\n"
    "     gpu_slice        GPU zones: id, ts, dur (GPU clock), name, depth, parent_id, context_id,\n"
    "                      context_name, context_type, utid, cpu_ts, cpu_dur, source_location_id, file, line, function\n"
    "     message          id, ts, utid, text, color, severity, source\n"
    "     lock             id, name, type, is_contended, ts_announce, ts_terminate, file, line,\n"
    "                      event_count, thread_count, wait_count, wait_total\n"
    "     lock_event       id, lock_id, ts, type (Wait/Obtain/Release/...Shared), utid, tid\n"
    "     lock_wait        id, lock_id, utid, tid, ts, dur (Wait -> Obtain), shared\n"
    "     memory_event     id, pool_id, pool, ptr, size, ts, dur, ts_free, utid, utid_free\n"
    "   Functions: trace_start(), trace_end(), trace_dur(), regexp(pattern, text[, 'i']),\n"
    "   plus SQLite's own (including math functions, median() and percentile()).\n"
    "\n"
    "5. Rules of thumb.\n"
    "   - This is SQLite, not PerfettoSQL. INCLUDE PERFETTO MODULE and CREATE [OR REPLACE]\n"
    "     PERFETTO TABLE/VIEW are accepted; CREATE PERFETTO FUNCTION/MACRO, the stdlib and\n"
    "     table functions such as ancestor_slice() are not available.\n"
    "   - slice.name is the source location's name (zone name, or the function if unnamed).\n"
    "   - dur = -1 means the zone was still open at the end of the trace.\n"
    "   - Join on utid, never tid. In slice, track_id == utid. Fibers are threads with is_fiber = 1.\n"
    "   - Fast filters on slice: id, parent_id, utid / track_id / tid / thread_name,\n"
    "     name / source_location_id / file / function, depth, and ts ranges (ts >= A AND ts < B).\n"
    "     GROUP BY name (or other source location columns), GROUP BY utid and GROUP BY parent_id\n"
    "     stream without sorting. Other GROUP BYs over all zones sort millions of rows; filter first.\n"
    "   - Children of a zone: WHERE parent_id = X. Self time: dur minus the sum of the children's dur.\n"
    "   - Aggregate (COUNT/GROUP BY/LIMIT) rather than dumping raw rows.\n";

static void Die( const std::string& msg )
{
    fprintf( stderr, "tracy-sql: %s\n", msg.c_str() );
    exit( 2 );
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

struct StdioSink : Sink
{
    bool quiet = false;
    void Out( const char* data, size_t len ) override { fwrite( data, 1, len, stdout ); }
    void Err( const char* data, size_t len ) override
    {
        if( quiet && strncmp( data, "Query executed", 14 ) == 0 ) return;
        fflush( stdout );
        fwrite( data, 1, len, stderr );
    }
};

static bool WriteAll( int fd, const char* data, size_t len )
{
    while( len > 0 )
    {
        const ssize_t n = write( fd, data, len );
        if( n < 0 )
        {
            if( errno == EINTR ) continue;
            return false;
        }
        data += n;
        len -= size_t( n );
    }
    return true;
}

static bool ReadAll( int fd, char* data, size_t len )
{
    while( len > 0 )
    {
        const ssize_t n = read( fd, data, len );
        if( n < 0 && errno == EINTR ) continue;
        if( n <= 0 ) return false;
        data += n;
        len -= size_t( n );
    }
    return true;
}

static bool ReadLine( int fd, std::string& line )
{
    line.clear();
    char c;
    for(;;)
    {
        const ssize_t n = read( fd, &c, 1 );
        if( n < 0 && errno == EINTR ) continue;
        if( n <= 0 ) return false;
        if( c == '\n' ) return true;
        line.push_back( c );
        if( line.size() > 4096 ) return false;
    }
}

// Wire format, server to client: frames "O <len>\n<bytes>" (stdout), "E <len>\n<bytes>" (stderr), then "X <exit code>\n".
struct SocketSink : Sink
{
    int fd;
    bool ok = true;
    void Frame( char type, const char* data, size_t len )
    {
        if( !ok || len == 0 ) return;
        char head[32];
        const int n = snprintf( head, sizeof( head ), "%c %zu\n", type, len );
        ok = WriteAll( fd, head, size_t( n ) ) && WriteAll( fd, data, len );
    }
    void Out( const char* data, size_t len ) override { Frame( 'O', data, len ); }
    void Err( const char* data, size_t len ) override { Frame( 'E', data, len ); }
};

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

static std::string SessionDir()
{
    const char* tmp = getenv( "TMPDIR" );
    std::string dir = tmp && *tmp ? tmp : "/tmp";
    while( dir.size() > 1 && dir.back() == '/' ) dir.pop_back();
    dir += "/tracy-sql-" + std::to_string( getuid() );
    // sun_path is short (104 bytes on macOS); fall back to /tmp for deep TMPDIRs.
    if( dir.size() + 40 > sizeof( sockaddr_un::sun_path ) ) dir = "/tmp/tracy-sql-" + std::to_string( getuid() );
    return dir;
}

static std::string SocketPath( const std::string& name )
{
    if( name.find( '/' ) != std::string::npos ) return name;
    return SessionDir() + "/" + name + ".sock";
}

static int Connect( const std::string& path )
{
    sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    if( path.size() >= sizeof( addr.sun_path ) ) return -1;
    strcpy( addr.sun_path, path.c_str() );
    const int fd = socket( AF_UNIX, SOCK_STREAM, 0 );
    if( fd < 0 ) return -1;
    if( connect( fd, (sockaddr*)&addr, sizeof( addr ) ) != 0 )
    {
        close( fd );
        return -1;
    }
    return fd;
}

// Sends a request and relays the reply. Returns the remote exit code, or -1 if the session cannot be reached.
static int Request( const std::string& path, const std::string& request, bool relay )
{
    const int fd = Connect( path );
    if( fd < 0 ) return -1;
    if( !WriteAll( fd, request.data(), request.size() ) )
    {
        close( fd );
        return -1;
    }
    std::string line, payload;
    int code = -1;
    while( ReadLine( fd, line ) )
    {
        if( line.size() < 3 ) break;
        if( line[0] == 'X' )
        {
            code = atoi( line.c_str() + 2 );
            break;
        }
        const size_t len = strtoull( line.c_str() + 2, nullptr, 10 );
        payload.resize( len );
        if( !ReadAll( fd, payload.data(), len ) ) break;
        if( !relay ) continue;
        if( line[0] == 'O' )
        {
            fwrite( payload.data(), 1, len, stdout );
        }
        else
        {
            fflush( stdout );
            fwrite( payload.data(), 1, len, stderr );
        }
    }
    close( fd );
    return code;
}

static std::string g_socketPath;

static void OnSignal( int )
{
    if( !g_socketPath.empty() ) unlink( g_socketPath.c_str() );
    _exit( 0 );
}

static int64_t ParseDuration( const std::string& s )
{
    if( s == "0" || s == "never" ) return 0;
    if( s == "auto" ) return 30 * 60;
    char* end = nullptr;
    const double v = strtod( s.c_str(), &end );
    if( end == s.c_str() || v < 0 ) Die( "bad --idle-timeout '" + s + "' (use e.g. 30m, 90s, 2h, 0)" );
    switch( *end )
    {
    case 'h': return int64_t( v * 3600 );
    case 'm': return int64_t( v * 60 );
    case 's': case 0: return int64_t( v );
    default: Die( "bad --idle-timeout '" + s + "' (use e.g. 30m, 90s, 2h, 0)" );
    }
    return 0;
}

static TraceDb* Load( const std::string& trace, bool quiet, std::string& err )
{
    const auto t0 = std::chrono::steady_clock::now();
    auto db = OpenTrace( trace.c_str(), err );
    if( !db ) return nullptr;
    if( !quiet )
    {
        const double sec = std::chrono::duration<double>( std::chrono::steady_clock::now() - t0 ).count();
        struct stat st {};
        stat( trace.c_str(), &st );
        fprintf( stderr, "Trace loaded: %.2f MB in %.2fs\n", double( st.st_size ) / 1e6, sec );
    }
    return db;
}

static void HandleClient( TraceDb* db, int fd, bool& quit )
{
    std::string line;
    if( !ReadLine( fd, line ) ) return;
    SocketSink sink;
    sink.fd = fd;
    char cmd = 0;
    int fmt = 0, agent = 0;
    long long maxRows = 0, maxBytes = 0, maxCell = 0;
    unsigned long long len = 0;
    const int n = sscanf( line.c_str(), "TSQL1 %c %d %d %lld %lld %lld %llu", &cmd, &fmt, &agent, &maxRows, &maxBytes, &maxCell, &len );
    int code = 2;
    if( n >= 1 && cmd == 'K' )
    {
        quit = true;
        code = 0;
    }
    else if( n >= 1 && cmd == 'P' )
    {
        code = 0;
    }
    else if( n == 7 && cmd == 'Q' && len < ( 1ull << 30 ) )
    {
        std::string sql( size_t( len ), 0 );
        if( !ReadAll( fd, sql.data(), sql.size() ) ) return;
        OutputOptions o;
        o.format = fmt ? OutputOptions::Format::Markdown : OutputOptions::Format::Csv;
        o.agent = agent != 0;
        o.maxRows = maxRows;
        o.maxBytes = maxBytes;
        o.maxCellWidth = maxCell;
        code = RunSql( GetSqlite( db ), sql, o, sink );
    }
    else
    {
        const std::string msg = "tracy-sql: bad request (client and server versions differ?)\n";
        sink.Err( msg.data(), msg.size() );
    }
    char tail[32];
    const int tn = snprintf( tail, sizeof( tail ), "X %d\n", code );
    WriteAll( fd, tail, size_t( tn ) );
}

static int ServerUnix( const std::string& name, const std::string& trace, bool daemonize, int64_t idleSec )
{
    const std::string path = SocketPath( name );
    if( path.size() >= sizeof( sockaddr_un::sun_path ) ) Die( "socket path too long: " + path );
    if( name.find( '/' ) == std::string::npos )
    {
        const std::string dir = SessionDir();
        mkdir( dir.c_str(), 0700 );
    }
    if( access( path.c_str(), F_OK ) == 0 )
    {
        if( Request( path, "TSQL1 P\n", false ) == 0 ) Die( "session '" + name + "' is already running (tracy-sql server kill " + name + ")" );
        unlink( path.c_str() );     // stale
    }
    if( access( trace.c_str(), R_OK ) != 0 ) Die( "cannot read " + trace );

    // Fork before loading: the loader starts threads.
    int ready[2] = { -1, -1 };
    if( daemonize )
    {
        if( pipe( ready ) != 0 ) Die( "pipe failed" );
        const pid_t pid = fork();
        if( pid < 0 ) Die( "fork failed" );
        if( pid > 0 )
        {
            close( ready[1] );
            std::string msg;
            char buf[512];
            ssize_t n;
            while( ( n = read( ready[0], buf, sizeof( buf ) ) ) > 0 ) msg.append( buf, size_t( n ) );
            if( msg.rfind( "OK", 0 ) == 0 )
            {
                printf( "Session '%s' ready (pid %d, socket %s)%s", name.c_str(), int( pid ), path.c_str(), msg.c_str() + 2 );
                printf( "Query it with: tracy-sql query --remote %s \"SELECT ...\"; stop it with: tracy-sql server kill %s\n", name.c_str(), name.c_str() );
                return 0;
            }
            fprintf( stderr, "tracy-sql: session did not start: %s\n", msg.empty() ? "server exited" : msg.c_str() );
            return 1;
        }
        close( ready[0] );
        setsid();
        const std::string log = path + ".log";
        const int nul = open( "/dev/null", O_RDONLY );
        const int lfd = open( log.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600 );
        if( nul >= 0 ) dup2( nul, 0 );
        if( lfd >= 0 ) { dup2( lfd, 1 ); dup2( lfd, 2 ); }
    }

    const auto Fail = [&]( const std::string& msg ) {
        if( daemonize )
        {
            WriteAll( ready[1], msg.data(), msg.size() );
            unlink( ( path + ".log" ).c_str() );
        }
        else
        {
            fprintf( stderr, "tracy-sql: %s\n", msg.c_str() );
        }
        _exit( 1 );
    };

    const auto t0 = std::chrono::steady_clock::now();
    std::string err;
    TraceDb* db = Load( trace, false, err );
    if( !db ) Fail( err );
    const double loadSec = std::chrono::duration<double>( std::chrono::steady_clock::now() - t0 ).count();

    sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    strcpy( addr.sun_path, path.c_str() );
    const int lfd = socket( AF_UNIX, SOCK_STREAM, 0 );
    if( lfd < 0 || bind( lfd, (sockaddr*)&addr, sizeof( addr ) ) != 0 || listen( lfd, 16 ) != 0 ) Fail( "cannot listen on " + path + ": " + strerror( errno ) );
    chmod( path.c_str(), 0600 );
    g_socketPath = path;
    signal( SIGINT, OnSignal );
    signal( SIGTERM, OnSignal );
    signal( SIGPIPE, SIG_IGN );

    if( daemonize )
    {
        char msg[128];
        snprintf( msg, sizeof( msg ), "OK, trace loaded in %.2fs\n", loadSec );
        WriteAll( ready[1], msg, strlen( msg ) );
        close( ready[1] );
    }
    else
    {
        fprintf( stderr, "Session '%s' serving on %s. Ctrl-C to stop.\n", name.c_str(), path.c_str() );
    }

    bool quit = false;
    while( !quit )
    {
        pollfd pfd { lfd, POLLIN, 0 };
        const int pr = poll( &pfd, 1, idleSec > 0 ? int( std::min<int64_t>( idleSec * 1000, INT32_MAX ) ) : -1 );
        if( pr < 0 && errno == EINTR ) continue;
        if( pr == 0 )
        {
            fprintf( stderr, "idle timeout, exiting\n" );
            break;
        }
        const int fd = accept( lfd, nullptr, nullptr );
        if( fd < 0 ) continue;
        HandleClient( db, fd, quit );
        close( fd );
    }
    unlink( path.c_str() );
    if( daemonize ) unlink( ( path + ".log" ).c_str() );
    fflush( nullptr );
    _exit( 0 );
}

static int ServerKill( const std::string& name )
{
    const std::string path = SocketPath( name );
    if( Request( path, "TSQL1 K\n", false ) != 0 )
    {
        fprintf( stderr, "tracy-sql: no running session '%s' (%s)\n", name.c_str(), path.c_str() );
        unlink( path.c_str() );
        return 1;
    }
    printf( "Session '%s' stopped\n", name.c_str() );
    return 0;
}

static int ServerList()
{
    const std::string dir = SessionDir();
    DIR* d = opendir( dir.c_str() );
    int found = 0;
    if( d )
    {
        while( auto e = readdir( d ) )
        {
            const std::string f = e->d_name;
            if( f.size() < 6 || f.compare( f.size() - 5, 5, ".sock" ) != 0 ) continue;
            if( Request( dir + "/" + f, "TSQL1 P\n", false ) != 0 ) continue;
            printf( "%s\n", f.substr( 0, f.size() - 5 ).c_str() );
            found++;
        }
        closedir( d );
    }
    if( !found ) fprintf( stderr, "no running sessions\n" );
    return 0;
}

// ---------------------------------------------------------------------------

static std::string ReadFile( const std::string& path )
{
    FILE* f = path == "-" ? stdin : fopen( path.c_str(), "rb" );
    if( !f ) Die( "cannot read " + path );
    std::string s;
    char buf[65536];
    size_t n;
    while( ( n = fread( buf, 1, sizeof( buf ), f ) ) > 0 ) s.append( buf, n );
    if( f != stdin ) fclose( f );
    return s;
}

static int64_t ParseInt( const char* flag, const std::string& v )
{
    char* end = nullptr;
    const long long r = strtoll( v.c_str(), &end, 10 );
    if( end == v.c_str() || *end || r < 0 ) Die( std::string( "bad value for " ) + flag + ": " + v );
    return r;
}

static int CmdQuery( std::vector<std::string> args )
{
    std::string file, remote, format;
    std::vector<std::string> pos;
    int agentFlag = -1;
    int64_t maxRows = -1, maxBytes = -1, maxCell = -1;
    bool quiet = false;
    for( size_t i = 0; i < args.size(); i++ )
    {
        std::string a = args[i];
        std::string inlineVal;
        bool hasInline = false;
        if( a.rfind( "--", 0 ) == 0 )
        {
            const auto eq = a.find( '=' );
            if( eq != std::string::npos )
            {
                inlineVal = a.substr( eq + 1 );
                a = a.substr( 0, eq );
                hasInline = true;
            }
        }
        const auto Val = [&]() -> std::string {
            if( hasInline ) return inlineVal;
            if( i + 1 >= args.size() ) Die( "missing value for " + a );
            return args[++i];
        };
        if( a == "-f" || a == "--query-file" ) file = Val();
        else if( a == "--remote" ) remote = Val();
        else if( a == "--format" ) format = Val();
        else if( a == "--agent" ) agentFlag = 1;
        else if( a == "--no-agent" ) agentFlag = 0;
        else if( a == "--max-rows" ) maxRows = ParseInt( "--max-rows", Val() );
        else if( a == "--max-bytes" ) maxBytes = ParseInt( "--max-bytes", Val() );
        else if( a == "--max-cell-width" ) maxCell = ParseInt( "--max-cell-width", Val() );
        else if( a == "--quiet" || a == "--no-progress" ) quiet = true;
        else if( a == "-h" || a == "--help" ) { fputs( Usage, stdout ); return 0; }
        else if( a == "--" ) { for( i++; i < args.size(); i++ ) pos.push_back( args[i] ); }
        else if( a.size() > 1 && a[0] == '-' && a != "-" ) Die( "unknown flag " + a + " (see tracy-sql help)" );
        else pos.push_back( args[i] );
    }

    std::string trace, sql;
    size_t pi = 0;
    if( remote.empty() )
    {
        if( pos.empty() ) Die( "query needs a trace file or --remote NAME (see tracy-sql help)" );
        trace = pos[pi++];
    }
    if( !file.empty() )
    {
        if( pi < pos.size() ) Die( "give SQL either as an argument or with -f, not both" );
        sql = ReadFile( file );
    }
    else if( pi < pos.size() )
    {
        sql = pos[pi++];
        if( pi < pos.size() ) Die( "unexpected argument '" + pos[pi] + "' (quote the SQL as one argument)" );
    }
    else
    {
        if( isatty( 0 ) ) Die( "no SQL given (pass it as an argument, with -f FILE, or on stdin)" );
        sql = ReadFile( "-" );
    }

    if( !format.empty() && format != "csv" && format != "markdown" && format != "md" ) Die( "unknown --format '" + format + "' (csv or markdown)" );
    const bool envAgent = ( getenv( "CLAUDECODE" ) || getenv( "AI_AGENT" ) ) && !isatty( 1 ) && format.empty();
    const bool agent = agentFlag >= 0 ? agentFlag == 1 : envAgent;

    OutputOptions o;
    o.agent = agent;
    o.format = format.empty() ? ( agent ? OutputOptions::Format::Markdown : OutputOptions::Format::Csv )
                              : ( format == "csv" ? OutputOptions::Format::Csv : OutputOptions::Format::Markdown );
    const bool md = o.format == OutputOptions::Format::Markdown;
    o.maxRows = maxRows >= 0 ? maxRows : ( md ? 1000 : 0 );
    o.maxBytes = maxBytes >= 0 ? maxBytes : ( md ? 10000 : 0 );
    o.maxCellWidth = maxCell >= 0 ? maxCell : ( md ? 500 : 0 );

    if( !remote.empty() )
    {
        char head[160];
        snprintf( head, sizeof( head ), "TSQL1 Q %d %d %lld %lld %lld %zu\n", md ? 1 : 0, agent ? 1 : 0,
                  (long long)o.maxRows, (long long)o.maxBytes, (long long)o.maxCellWidth, sql.size() );
        const int code = Request( SocketPath( remote ), std::string( head ) + sql, true );
        if( code < 0 )
        {
            const std::string msg = "no running session '" + remote + "'; start one with: tracy-sql server unix --name " + remote + " --daemonize TRACE.tracy";
            if( agent ) fprintf( stderr, "{\"error\":\"%s\"}\n", msg.c_str() );
            else fprintf( stderr, "tracy-sql: %s\n", msg.c_str() );
            return 1;
        }
        return code;
    }

    std::string err;
    TraceDb* db = Load( trace, quiet, err );
    if( !db )
    {
        if( agent )
        {
            std::string esc;
            for( char c : err ) { if( c == '"' || c == '\\' ) esc.push_back( '\\' ); esc.push_back( c ); }
            fprintf( stderr, "{\"error\":\"%s\"}\n", esc.c_str() );
        }
        else
        {
            fprintf( stderr, "tracy-sql: %s\n", err.c_str() );
        }
        return 1;
    }
    StdioSink sink;
    sink.quiet = quiet;
    const int code = RunSql( GetSqlite( db ), sql, o, sink );
    // Skip tearing down gigabytes of trace data.
    fflush( nullptr );
    _exit( code );
}

static int CmdServer( std::vector<std::string> args )
{
    std::string name = "tracy", path, idle = "auto";
    bool daemonize = false;
    std::vector<std::string> pos;
    for( size_t i = 0; i < args.size(); i++ )
    {
        std::string a = args[i];
        std::string inlineVal;
        bool hasInline = false;
        const auto eq = a.find( '=' );
        if( a.rfind( "--", 0 ) == 0 && eq != std::string::npos )
        {
            inlineVal = a.substr( eq + 1 );
            a = a.substr( 0, eq );
            hasInline = true;
        }
        const auto Val = [&]() -> std::string {
            if( hasInline ) return inlineVal;
            if( i + 1 >= args.size() ) Die( "missing value for " + a );
            return args[++i];
        };
        if( a == "--name" ) name = Val();
        else if( a == "--path" ) path = Val();
        else if( a == "--idle-timeout" ) idle = Val();
        else if( a == "--daemonize" ) daemonize = true;
        else if( a == "--quiet" || a == "--no-progress" ) {}
        else if( a == "-h" || a == "--help" ) { fputs( Usage, stdout ); return 0; }
        else if( a.size() > 1 && a[0] == '-' ) Die( "unknown flag " + a + " (see tracy-sql help)" );
        else pos.push_back( args[i] );
    }
    if( pos.empty() ) Die( "server needs a mode: unix, kill or list" );
    const std::string mode = pos[0];
    if( !path.empty() ) name = path;
    if( mode == "unix" )
    {
        if( pos.size() != 2 ) Die( "usage: tracy-sql server unix [--name NAME] [--daemonize] TRACE.tracy" );
        if( name.find( '/' ) == std::string::npos && name.find_first_not_of( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" ) != std::string::npos ) Die( "bad session name '" + name + "'" );
        return ServerUnix( name, pos[1], daemonize, ParseDuration( idle ) );
    }
    if( mode == "kill" )
    {
        if( pos.size() == 2 ) name = pos[1];
        return ServerKill( name );
    }
    if( mode == "list" ) return ServerList();
    Die( "unknown server mode '" + mode + "' (unix, kill or list; http and stdio are not implemented)" );
    return 2;
}

int main( int argc, char** argv )
{
    std::vector<std::string> args( argv + 1, argv + argc );
    if( args.empty() || args[0] == "-h" || args[0] == "--help" )
    {
        fputs( Usage, args.empty() ? stderr : stdout );
        return args.empty() ? 2 : 0;
    }
    const std::string cmd = args[0];
    args.erase( args.begin() );
    if( cmd == "help" )
    {
        fputs( !args.empty() && args[0] == "agent" ? AgentHelp : Usage, stdout );
        return 0;
    }
    if( cmd == "-v" || cmd == "--version" || cmd == "version" )
    {
        printf( "tracy-sql %i.%i.%i / %s\n", tracy::Version::Major, tracy::Version::Minor, tracy::Version::Patch, tracy::GitRef );
        return 0;
    }
    if( cmd == "query" ) return CmdQuery( std::move( args ) );
    if( cmd == "server" ) return CmdServer( std::move( args ) );
    Die( "unknown command '" + cmd + "' (query, server, help)" );
    return 2;
}
