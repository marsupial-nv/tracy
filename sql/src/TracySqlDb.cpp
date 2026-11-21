// SQLite virtual tables over a tracy::Worker.
//
// The zone table (slice) is never copied into SQLite. Tracy keeps zones as a
// tree per thread: every thread has a vector of root zones and every zone with
// children owns one child vector. ZoneIndex numbers those vectors ("containers")
// breadth-first per thread and gives each zone the id
//
//     id = base[container] + position in container
//
// so ids are dense (0..N-1), stable for a given file, contiguous per thread,
// and all children of one zone are an id range. Per container we keep 16 bytes
// (base, parent id, Tracy child index, utid, depth); nothing is kept per zone
// unless a query needs rows by source location, in which case a 4 byte/zone
// list of ids bucketed by source location is built once (lazily).

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <sqlite3.h>

#include "../../server/TracyFileRead.hpp"
#include "../../server/TracyWorker.hpp"
#include "../../public/common/TracyVersion.hpp"

#include "TracySql.hpp"

using namespace tracy;

namespace tracysql
{

using ZoneVec = Vector<short_ptr<ZoneEvent>>;

static inline const ZoneEvent& ZoneAt( const ZoneVec& v, size_t i )
{
    // Vectors loaded from a file hold the events inline ("magic"); live ones hold pointers.
    return v.is_magic() ? ( *(const Vector<ZoneEvent>*)&v )[i] : *v[i].get();
}

static const uint32_t NoParent = 0xFFFFFFFF;

struct Cont
{
    uint32_t base;      // id of the first zone
    uint32_t parent;    // id of the owning zone, NoParent for thread roots
    int32_t child;      // Tracy child vector index, -1 for thread roots
    uint16_t utid;
    uint16_t depth;
};

struct SrcInfo
{
    const char* name = nullptr;
    const char* function = nullptr;
    const char* file = nullptr;
    uint32_t line = 0;
    uint32_t color = 0;
    bool known = false;
};

struct ThreadInfo
{
    uint64_t tid;
    const char* name;
    bool isFiber;
    uint64_t count;
    int64_t start, end;
    bool hasTime;
};

struct TraceDb
{
    std::unique_ptr<Worker> worker;
    sqlite3* db = nullptr;
    std::string path;
    double loadSec = 0;

    std::vector<ThreadInfo> threads;
    std::unordered_map<uint64_t, int> tidToUtid;

    // --- zone index ---
    bool indexReady = false;
    std::vector<Cont> conts;                // + sentinel with base == total
    std::vector<uint32_t> childToCont;
    std::vector<uint32_t> threadFirstCont;  // size threads+1
    uint64_t total = 0;
    std::vector<uint64_t> srclocCount;      // 65536
    std::vector<SrcInfo> src;               // 65536, indexed by uint16_t( srcloc )
    std::vector<uint16_t> usedSrc;          // source locations with zones
    std::vector<int16_t> allSrc;            // every known source location
    std::unordered_map<std::string, std::vector<uint16_t>> byName, byFile, byFunction;

    bool srcIndexReady = false;
    std::vector<uint32_t> srcIds;           // zone ids grouped by source location, ascending in each group
    std::vector<uint64_t> srcStart;         // 65537

    const Worker& W() const { return *worker; }

    int UtidOfTid( uint64_t tid ) const
    {
        auto it = tidToUtid.find( tid );
        return it == tidToUtid.end() ? -1 : it->second;
    }
    int UtidOfCompressed( uint16_t thread ) const { return UtidOfTid( W().DecompressThread( thread ) ); }

    const ZoneVec& Vec( const Cont& c ) const
    {
        return c.child < 0 ? W().GetThreadData()[c.utid]->timeline : W().GetZoneChildren( c.child );
    }
    uint32_t ContSize( uint32_t ci ) const { return conts[ci+1].base - conts[ci].base; }

    uint32_t FindCont( uint32_t id, uint32_t hint ) const
    {
        const uint32_t n = uint32_t( conts.size() - 1 );
        if( hint < n && conts[hint].base <= id )
        {
            uint32_t h = hint;
            int steps = 0;
            while( conts[h+1].base <= id )
            {
                h++;
                if( ++steps > 8 )
                {
                    auto it = std::upper_bound( conts.begin() + h, conts.end(), id, []( uint32_t v, const Cont& c ) { return v < c.base; } );
                    return uint32_t( it - conts.begin() ) - 1;
                }
            }
            return h;
        }
        auto it = std::upper_bound( conts.begin(), conts.end(), id, []( uint32_t v, const Cont& c ) { return v < c.base; } );
        return uint32_t( it - conts.begin() ) - 1;
    }

    void FillSrc( int16_t id )
    {
        auto& s = src[uint16_t( id )];
        if( s.known ) return;
        const auto& sl = W().GetSourceLocation( id );
        s.name = W().GetZoneName( sl );
        s.function = W().GetString( sl.function );
        s.file = W().GetString( sl.file );
        s.line = sl.line;
        s.color = sl.color;
        s.known = true;
        allSrc.push_back( id );
    }

    void BuildIndex();
    void BuildSrcIndex();
};

void TraceDb::BuildIndex()
{
    if( indexReady ) return;
    indexReady = true;

    const auto& w = W();
    const auto& td = w.GetThreadData();
    srclocCount.assign( 65536, 0 );
    src.assign( 65536, SrcInfo() );
    threadFirstCont.reserve( td.size() + 1 );
    conts.reserve( size_t( w.GetZoneCount() / 4 ) + td.size() + 16 );

    uint64_t running = 0;
    for( size_t t = 0; t < td.size(); t++ )
    {
        threadFirstCont.push_back( uint32_t( conts.size() ) );
        conts.push_back( Cont { 0, NoParent, -1, uint16_t( t ), 0 } );
        // conts doubles as the BFS queue
        for( size_t ci = threadFirstCont.back(); ci < conts.size(); ci++ )
        {
            Cont c = conts[ci];
            c.base = uint32_t( running );
            conts[ci].base = c.base;
            const auto& v = Vec( c );
            const size_t sz = v.size();
            const uint16_t nextDepth = uint16_t( c.depth == 0xFFFF ? 0xFFFF : c.depth + 1 );
            if( v.is_magic() )
            {
                const ZoneEvent* z = ( (const Vector<ZoneEvent>*)&v )->data();
                for( size_t i = 0; i < sz; i++, z++ )
                {
                    srclocCount[uint16_t( z->SrcLoc() )]++;
                    if( z->HasChildren() ) conts.push_back( Cont { 0, uint32_t( running + i ), z->Child(), c.utid, nextDepth } );
                }
            }
            else
            {
                for( size_t i = 0; i < sz; i++ )
                {
                    const ZoneEvent* z = v[i].get();
                    srclocCount[uint16_t( z->SrcLoc() )]++;
                    if( z->HasChildren() ) conts.push_back( Cont { 0, uint32_t( running + i ), z->Child(), c.utid, nextDepth } );
                }
            }
            running += sz;
        }
    }
    threadFirstCont.push_back( uint32_t( conts.size() ) );
    total = running;
    conts.push_back( Cont { uint32_t( running ), NoParent, -1, 0, 0 } );
    assert( running < 0xFFFFFFFFull );

    int32_t maxChild = -1;
    for( auto& c : conts ) maxChild = std::max( maxChild, c.child );
    childToCont.assign( size_t( maxChild + 1 ), 0 );
    for( size_t i = 0; i + 1 < conts.size(); i++ ) if( conts[i].child >= 0 ) childToCont[conts[i].child] = uint32_t( i );

    for( uint32_t i = 0; i < 65536; i++ )
    {
        if( srclocCount[i] == 0 ) continue;
        FillSrc( int16_t( uint16_t( i ) ) );
        usedSrc.push_back( uint16_t( i ) );
    }
    for( const auto& kv : w.GetSourceLocationZones() ) if( kv.first != std::numeric_limits<int16_t>::max() ) FillSrc( kv.first );
    for( const auto& kv : w.GetGpuSourceLocationZones() ) if( kv.first != std::numeric_limits<int16_t>::max() ) FillSrc( kv.first );
    // Static source locations first (0, 1, ...), then dynamically allocated ones (-1, -2, ...).
    std::sort( allSrc.begin(), allSrc.end(), []( int16_t a, int16_t b ) { return ( a < 0 ) != ( b < 0 ) ? b < 0 : ( a < 0 ? a > b : a < b ); } );
    for( auto s : usedSrc )
    {
        byName[src[s].name].push_back( s );
        byFile[src[s].file].push_back( s );
        byFunction[src[s].function].push_back( s );
    }
}

void TraceDb::BuildSrcIndex()
{
    BuildIndex();
    if( srcIndexReady ) return;
    srcIndexReady = true;

    srcStart.assign( 65537, 0 );
    for( uint32_t i = 0; i < 65536; i++ ) srcStart[i+1] = srcStart[i] + srclocCount[i];
    std::vector<uint64_t> fill( srcStart.begin(), srcStart.end() - 1 );
    srcIds.resize( total );
    const size_t nc = conts.size() - 1;
    for( size_t ci = 0; ci < nc; ci++ )
    {
        const auto& c = conts[ci];
        const auto& v = Vec( c );
        const size_t sz = v.size();
        for( size_t i = 0; i < sz; i++ )
        {
            srcIds[fill[uint16_t( ZoneAt( v, i ).SrcLoc() )]++] = uint32_t( c.base + i );
        }
    }
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

static void ResultText( sqlite3_context* ctx, const char* s )
{
    if( s ) sqlite3_result_text( ctx, s, -1, SQLITE_STATIC );
    else sqlite3_result_null( ctx );
}

static void ResultUtid( sqlite3_context* ctx, int utid )
{
    if( utid >= 0 ) sqlite3_result_int( ctx, utid );
    else sqlite3_result_null( ctx );
}

enum class IntVal { Ok, Empty, Ignore };

// Reads the right-hand side of "column <op> value" for an integer column.
// For range operators the result is rounded outwards, which is safe because
// SQLite re-checks every row we return.
static IntVal GetIntArg( sqlite3_value* v, char op, int64_t& out )
{
    int type = sqlite3_value_numeric_type( v );
    if( type == SQLITE_NULL ) return IntVal::Empty;
    if( type == SQLITE_INTEGER )
    {
        out = sqlite3_value_int64( v );
        return IntVal::Ok;
    }
    if( type == SQLITE_FLOAT )
    {
        const double d = sqlite3_value_double( v );
        if( isnan( d ) ) return IntVal::Ignore;
        if( op == 'e' )
        {
            if( d != floor( d ) || fabs( d ) > 9e18 ) return IntVal::Empty;
            out = int64_t( d );
            return IntVal::Ok;
        }
        if( d > 9e18 ) { out = INT64_MAX; return IntVal::Ok; }
        if( d < -9e18 ) { out = INT64_MIN; return IntVal::Ok; }
        // g / G become ">= floor", l / L become "<= ceil"
        out = ( op == 'g' || op == 'G' ) ? int64_t( floor( d ) ) : int64_t( ceil( d ) );
        return IntVal::Ok;
    }
    return op == 'e' ? IntVal::Empty : IntVal::Ignore;
}

// ---------------------------------------------------------------------------
// slice
// ---------------------------------------------------------------------------

namespace SC
{
enum : int
{
    Id, Ts, Dur, TrackId, Category, Name, Depth, ParentId, ArgSetId, ThreadTs, ThreadDur,
    Utid, SrcLoc, File, Line, Function, ZoneName, Text, Color,
    ThreadName, Tid, IsMainThread, IsFiber, Cat, SliceId,
    Count
};
}

static const char* SliceSchema =
    "CREATE TABLE x("
    "id INTEGER, ts INTEGER, dur INTEGER, track_id INTEGER, category TEXT, name TEXT, depth INTEGER, parent_id INTEGER, "
    "arg_set_id INTEGER, thread_ts INTEGER, thread_dur INTEGER, "
    "utid INTEGER, source_location_id INTEGER, file TEXT, line INTEGER, function TEXT, zone_name TEXT, text TEXT, color INTEGER, "
    "thread_name TEXT HIDDEN, tid INTEGER HIDDEN, is_main_thread INTEGER HIDDEN, is_fiber INTEGER HIDDEN, cat TEXT HIDDEN, slice_id INTEGER HIDDEN)";

enum SliceMode : int { ModeRange = 0, ModeSrcEq = 1, ModeDfs = 2, ModeSrcOrdered = 3, ModeMask = 3, FlagRootsFirst = 4, FlagThreadOrdered = 8 };

struct SliceVtab : sqlite3_vtab
{
    TraceDb* t;
};

struct SliceCursor : sqlite3_vtab_cursor
{
    TraceDb* t;
    int mode = ModeRange;
    bool eof = true;

    // current row
    const ZoneEvent* z = nullptr;
    uint32_t id = 0;
    uint32_t cont = 0;

    // filters
    int64_t idLo, idHi;         // inclusive
    int64_t tsLo, tsHi;
    int64_t depthLo, depthHi;
    bool useThreadMask, useSrcSet;
    std::vector<uint8_t> threadMask;
    std::vector<uint8_t> srcSet;

    // range mode
    std::vector<std::pair<uint32_t, uint32_t>> ranges;
    size_t ri = 0;
    uint32_t rangeEnd = 0, contBase = 0, contEnd = 0;
    const ZoneVec* cv = nullptr;
    bool cvMagic = false;

    // source location mode
    std::vector<uint16_t> srcList;
    size_t si = 0;
    const uint32_t* p = nullptr;
    const uint32_t* pEnd = nullptr;

    // dfs mode
    struct Frame { uint32_t cont, pos, end; };
    std::vector<Frame> stack;
    std::vector<uint32_t> dfsThreads;
    size_t ti = 0;

    bool ThreadOk( uint16_t utid ) const { return !useThreadMask || threadMask[utid]; }

    bool RangeSeek( uint32_t cur )
    {
        for(;;)
        {
            if( cur >= rangeEnd )
            {
                if( ++ri >= ranges.size() ) return false;
                cur = ranges[ri].first;
                rangeEnd = ranges[ri].second;
                if( cur >= rangeEnd ) continue;
            }
            cont = t->FindCont( cur, cont );
            const Cont& c = t->conts[cont];
            const uint32_t cEnd = t->conts[cont+1].base;
            if( c.depth < depthLo || c.depth > depthHi )
            {
                cur = cEnd;
                continue;
            }
            contBase = c.base;
            contEnd = std::min( cEnd, rangeEnd );
            cv = &t->Vec( c );
            cvMagic = cv->is_magic();
            id = cur;
            z = &ZoneAt( *cv, id - contBase );
            return true;
        }
    }

    void RangeNext()
    {
        id++;
        if( id < contEnd )
        {
            z = cvMagic ? z + 1 : &ZoneAt( *cv, id - contBase );
        }
        else
        {
            eof = !RangeSeek( id );
        }
    }

    void SrcNext()
    {
        for(;;)
        {
            if( p == pEnd )
            {
                if( si >= srcList.size() ) { eof = true; return; }
                const uint16_t s = srcList[si++];
                const uint32_t* b = t->srcIds.data() + t->srcStart[s];
                const uint32_t* e = t->srcIds.data() + t->srcStart[s+1];
                if( idLo > 0 ) b = std::lower_bound( b, e, uint32_t( idLo ) );
                if( idHi < int64_t( t->total ) - 1 ) e = std::upper_bound( b, e, uint32_t( idHi ) );
                p = b;
                pEnd = e;
                cont = 0;
                continue;
            }
            const uint32_t i = *p++;
            cont = t->FindCont( i, cont );
            const Cont& c = t->conts[cont];
            if( c.depth < depthLo || c.depth > depthHi ) continue;
            if( !ThreadOk( c.utid ) ) continue;
            const ZoneEvent* e = &ZoneAt( t->Vec( c ), i - c.base );
            const int64_t s = e->Start();
            if( s < tsLo || s > tsHi ) continue;
            z = e;
            id = i;
            return;
        }
    }

    void DfsPush( uint32_t ci )
    {
        const uint32_t n = t->ContSize( ci );
        if( n == 0 ) return;
        uint32_t pos = 0;
        if( tsLo != INT64_MIN )
        {
            // First zone starting at or after tsLo; the one before it may still contain matching children.
            const auto& v = t->Vec( t->conts[ci] );
            uint32_t lo = 0, hi = n;
            while( lo < hi )
            {
                const uint32_t mid = ( lo + hi ) / 2;
                if( ZoneAt( v, mid ).Start() < tsLo ) lo = mid + 1;
                else hi = mid;
            }
            pos = lo > 0 ? lo - 1 : 0;
        }
        stack.push_back( Frame { ci, pos, n } );
    }

    void DfsNext()
    {
        for(;;)
        {
            if( stack.empty() )
            {
                if( ti >= dfsThreads.size() ) { eof = true; return; }
                DfsPush( t->threadFirstCont[dfsThreads[ti++]] );
                continue;
            }
            Frame& f = stack.back();
            if( f.pos >= f.end ) { stack.pop_back(); continue; }
            const uint32_t ci = f.cont;
            const Cont& c = t->conts[ci];
            const ZoneEvent* e = &ZoneAt( t->Vec( c ), f.pos );
            const int64_t s = e->Start();
            if( s > tsHi ) { stack.pop_back(); continue; }
            const uint32_t pos = f.pos++;
            const int64_t zid = int64_t( c.base ) + pos;
            const bool emit = s >= tsLo && c.depth >= depthLo && c.depth <= depthHi && zid >= idLo && zid <= idHi &&
                              ( !useSrcSet || srcSet[uint16_t( e->SrcLoc() )] );
            if( e->HasChildren() && int64_t( c.depth ) + 1 <= depthHi && !( e->IsEndValid() && e->End() < tsLo ) )
            {
                DfsPush( t->childToCont[e->Child()] );   // invalidates f
            }
            if( emit )
            {
                z = e;
                cont = ci;
                id = uint32_t( zid );
                return;
            }
        }
    }
};

static int SliceConnect( sqlite3* db, void* aux, int, const char* const*, sqlite3_vtab** out, char** )
{
    int rc = sqlite3_declare_vtab( db, SliceSchema );
    if( rc != SQLITE_OK ) return rc;
    sqlite3_vtab_config( db, SQLITE_VTAB_INNOCUOUS );
    auto v = new SliceVtab();
    memset( static_cast<sqlite3_vtab*>( v ), 0, sizeof( sqlite3_vtab ) );
    v->t = (TraceDb*)aux;
    *out = v;
    return SQLITE_OK;
}

static int SliceDisconnect( sqlite3_vtab* v )
{
    delete static_cast<SliceVtab*>( v );
    return SQLITE_OK;
}

static bool SliceOpSupported( int col, char op )
{
    switch( col )
    {
    case SC::Id:
    case SC::SliceId:
    case SC::Ts:
    case SC::Depth:
        return op != 'n';
    case SC::Utid:
    case SC::TrackId:
    case SC::Tid:
    case SC::ThreadName:
    case SC::Name:
    case SC::SrcLoc:
    case SC::File:
    case SC::Function:
        return op == 'e';
    case SC::ParentId:
        return op == 'e' || op == 'n';
    default:
        return false;
    }
}

static bool IsSrcCol( int col )
{
    return col == SC::Name || col == SC::SrcLoc || col == SC::File || col == SC::Function || col == SC::Line;
}

static bool IsThreadCol( int col )
{
    return col == SC::Utid || col == SC::TrackId || col == SC::ThreadName || col == SC::Tid || col == SC::IsMainThread || col == SC::IsFiber;
}

static int SliceBestIndex( sqlite3_vtab* vt, sqlite3_index_info* info )
{
    auto t = static_cast<SliceVtab*>( vt )->t;
    t->BuildIndex();

    std::string str;
    int argc = 0;
    bool idEq = false, parentEq = false, parentNull = false, srcEq = false, threadEq = false, tsLo = false, tsHi = false, depthAny = false, idRange = false;
    for( int i = 0; i < info->nConstraint; i++ )
    {
        const auto& c = info->aConstraint[i];
        if( !c.usable ) continue;
        int col = c.iColumn < 0 ? int( SC::Id ) : c.iColumn;
        char op;
        switch( c.op )
        {
        case SQLITE_INDEX_CONSTRAINT_EQ: op = 'e'; break;
        case SQLITE_INDEX_CONSTRAINT_GT: op = 'g'; break;
        case SQLITE_INDEX_CONSTRAINT_GE: op = 'G'; break;
        case SQLITE_INDEX_CONSTRAINT_LT: op = 'l'; break;
        case SQLITE_INDEX_CONSTRAINT_LE: op = 'L'; break;
        case SQLITE_INDEX_CONSTRAINT_ISNULL: op = 'n'; break;
        default: continue;
        }
        if( !SliceOpSupported( col, op ) ) continue;
        if( col == SC::SliceId ) col = SC::Id;
        info->aConstraintUsage[i].argvIndex = ++argc;
        info->aConstraintUsage[i].omit = 0;
        str.push_back( char( 'A' + col ) );
        str.push_back( op );
        switch( col )
        {
        case SC::Id: if( op == 'e' ) idEq = true; else idRange = true; break;
        case SC::Ts: if( op == 'e' ) tsLo = tsHi = true; else if( op == 'g' || op == 'G' ) tsLo = true; else tsHi = true; break;
        case SC::Depth: depthAny = true; break;
        case SC::ParentId: if( op == 'e' ) parentEq = true; else parentNull = true; break;
        case SC::Utid: case SC::TrackId: case SC::Tid: case SC::ThreadName: threadEq = true; break;
        default: srcEq = true; break;
        }
    }

    const double N = double( std::max<uint64_t>( t->total, 1 ) );
    double rows = N;
    int mode = ModeRange;
    double factor = 1.0;
    if( idEq )
    {
        rows = 1;
    }
    else if( parentEq )
    {
        rows = 8;
    }
    else
    {
        if( srcEq ) rows /= double( std::max<size_t>( t->usedSrc.size(), 1 ) );
        if( threadEq ) rows /= double( std::max<size_t>( t->threads.size(), 1 ) );
        if( tsLo && tsHi ) rows /= 100;
        else if( tsLo || tsHi ) rows /= 3;
        if( depthAny || parentNull ) rows /= 4;
        if( idRange ) rows /= 3;
        if( srcEq ) { mode = ModeSrcEq; factor = 2.0; }
        else if( tsLo || tsHi ) { mode = ModeDfs; factor = 1.5; }
    }
    if( rows < 1 ) rows = 1;

    // Output order. Range scans return ids ascending (threads ascending, a thread's
    // root zones first); source location scans can return any ordering of source
    // location attributes, which lets "GROUP BY name" stream without a sort.
    if( info->nOrderBy > 0 )
    {
        const int distinct = sqlite3_vtab_distinct( info );
        const bool grouped = distinct == 1 || distinct == 2;
        if( idEq )
        {
            info->orderByConsumed = 1;
        }
        else if( mode == ModeRange && !parentEq )
        {
            bool allSrc = true, allGroup = true, onlyParent = true;
            for( int i = 0; i < info->nOrderBy; i++ )
            {
                const int col = info->aOrderBy[i].iColumn;
                if( !IsSrcCol( col ) ) allSrc = false;
                if( col != SC::Id && col != SC::Utid && col != SC::TrackId && col != SC::ParentId && col != SC::SliceId && col >= 0 ) allGroup = false;
                if( col != SC::ParentId ) onlyParent = false;
            }
            if( allSrc )
            {
                mode = ModeSrcOrdered;
                factor = 2.0;
                str.push_back( '|' );
                for( int i = 0; i < info->nOrderBy; i++ )
                {
                    str.push_back( char( 'A' + info->aOrderBy[i].iColumn ) );
                    str.push_back( info->aOrderBy[i].desc ? 'd' : 'a' );
                }
                info->orderByConsumed = 1;
            }
            else
            {
                // Thread columns, optionally followed by "id ASC": visit threads in that order.
                const auto IsId = []( int c ) { return c < 0 || c == SC::Id || c == SC::SliceId; };
                int nThreadCols = 0;
                while( nThreadCols < info->nOrderBy && IsThreadCol( info->aOrderBy[nThreadCols].iColumn ) ) nThreadCols++;
                const int rest = info->nOrderBy - nThreadCols;
                const bool idTail = rest == 1 && IsId( info->aOrderBy[nThreadCols].iColumn ) && !info->aOrderBy[nThreadCols].desc;
                if( nThreadCols > 0 && ( rest == 0 || idTail ) )
                {
                    mode |= FlagThreadOrdered;
                    str.push_back( '|' );
                    for( int i = 0; i < nThreadCols; i++ )
                    {
                        str.push_back( char( 'A' + info->aOrderBy[i].iColumn ) );
                        str.push_back( info->aOrderBy[i].desc ? 'd' : 'a' );
                    }
                    info->orderByConsumed = 1;
                }
                else if( grouped && allGroup )
                {
                    if( onlyParent ) mode |= FlagRootsFirst;
                    info->orderByConsumed = 1;
                }
                else if( info->nOrderBy == 1 && idTail )
                {
                    info->orderByConsumed = 1;
                }
            }
        }
    }

    info->idxNum = mode;
    info->idxStr = sqlite3_mprintf( "%s", str.c_str() );
    info->needToFreeIdxStr = 1;
    info->estimatedRows = sqlite3_int64( rows );
    info->estimatedCost = rows * factor + 10;
    if( idEq ) info->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
    return SQLITE_OK;
}

static int SliceOpen( sqlite3_vtab* vt, sqlite3_vtab_cursor** out )
{
    auto c = new SliceCursor();
    memset( static_cast<sqlite3_vtab_cursor*>( c ), 0, sizeof( sqlite3_vtab_cursor ) );
    c->t = static_cast<SliceVtab*>( vt )->t;
    *out = c;
    return SQLITE_OK;
}

static int SliceClose( sqlite3_vtab_cursor* c )
{
    delete static_cast<SliceCursor*>( c );
    return SQLITE_OK;
}

static void IntersectSrc( SliceCursor* c, const std::vector<uint16_t>* list )
{
    std::vector<uint8_t> m( 65536, 0 );
    if( list ) for( auto s : *list ) m[s] = 1;
    if( c->useSrcSet ) for( size_t i = 0; i < 65536; i++ ) c->srcSet[i] &= m[i];
    else c->srcSet = std::move( m );
    c->useSrcSet = true;
}

static int SliceFilter( sqlite3_vtab_cursor* cur, int idxNum, const char* idxStr, int argc, sqlite3_value** argv )
{
    auto c = static_cast<SliceCursor*>( cur );
    auto t = c->t;
    t->BuildIndex();

    c->mode = idxNum & ModeMask;
    const bool rootsFirst = ( idxNum & FlagRootsFirst ) != 0;
    c->eof = true;
    c->idLo = 0;
    c->idHi = int64_t( t->total ) - 1;
    c->tsLo = INT64_MIN;
    c->tsHi = INT64_MAX;
    c->depthLo = 0;
    c->depthHi = INT64_MAX;
    c->useThreadMask = c->useSrcSet = false;
    c->ranges.clear();
    c->srcList.clear();
    c->stack.clear();
    c->dfsThreads.clear();
    c->ri = c->si = c->ti = 0;
    c->p = c->pEnd = nullptr;
    c->cont = 0;

    const size_t nThreads = t->threads.size();
    bool empty = false;
    const auto Bound = [&empty]( char op, int64_t v, int64_t& lo, int64_t& hi ) {
        switch( op )
        {
        case 'e': lo = std::max( lo, v ); hi = std::min( hi, v ); break;
        case 'g': if( v == INT64_MAX ) empty = true; else lo = std::max( lo, v + 1 ); break;
        case 'G': lo = std::max( lo, v ); break;
        case 'l': if( v == INT64_MIN ) empty = true; else hi = std::min( hi, v - 1 ); break;
        case 'L': hi = std::min( hi, v ); break;
        }
    };
    const auto ThreadMask = [&]( const std::function<bool( size_t )>& pred ) {
        if( !c->useThreadMask ) c->threadMask.assign( nThreads, 1 );
        c->useThreadMask = true;
        for( size_t i = 0; i < nThreads; i++ ) if( !pred( i ) ) c->threadMask[i] = 0;
    };

    const char* s = idxStr ? idxStr : "";
    for( int i = 0; i < argc && s[0] && s[0] != '|'; i++, s += 2 )
    {
        const int col = s[0] - 'A';
        const char op = s[1];
        sqlite3_value* v = argv[i];
        if( col == SC::ParentId && op == 'n' )
        {
            c->depthHi = std::min<int64_t>( c->depthHi, 0 );
            continue;
        }
        if( col == SC::Name || col == SC::File || col == SC::Function || col == SC::ThreadName )
        {
            if( sqlite3_value_type( v ) == SQLITE_NULL ) { empty = true; continue; }
            const char* txt = (const char*)sqlite3_value_text( v );
            if( !txt ) { empty = true; continue; }
            if( col == SC::ThreadName )
            {
                ThreadMask( [&]( size_t u ) { return strcmp( t->threads[u].name, txt ) == 0; } );
            }
            else
            {
                const auto& map = col == SC::Name ? t->byName : col == SC::File ? t->byFile : t->byFunction;
                auto it = map.find( txt );
                IntersectSrc( c, it == map.end() ? nullptr : &it->second );
            }
            continue;
        }
        int64_t iv;
        const auto st = GetIntArg( v, op, iv );
        if( st == IntVal::Empty ) { empty = true; continue; }
        if( st == IntVal::Ignore ) continue;
        switch( col )
        {
        case SC::Id: Bound( op, iv, c->idLo, c->idHi ); break;
        case SC::Ts: Bound( op, iv, c->tsLo, c->tsHi ); break;
        case SC::Depth: Bound( op, iv, c->depthLo, c->depthHi ); break;
        case SC::Utid:
        case SC::TrackId:
            ThreadMask( [&]( size_t u ) { return int64_t( u ) == iv; } );
            break;
        case SC::Tid:
            ThreadMask( [&]( size_t u ) { return int64_t( t->threads[u].tid ) == iv; } );
            break;
        case SC::SrcLoc:
        {
            std::vector<uint16_t> one;
            if( iv >= INT16_MIN && iv <= INT16_MAX ) one.push_back( uint16_t( int16_t( iv ) ) );
            IntersectSrc( c, &one );
            break;
        }
        case SC::ParentId:
            if( iv < 0 || iv >= int64_t( t->total ) )
            {
                empty = true;
            }
            else
            {
                const uint32_t pc = t->FindCont( uint32_t( iv ), 0 );
                const auto& pz = ZoneAt( t->Vec( t->conts[pc] ), uint32_t( iv ) - t->conts[pc].base );
                if( !pz.HasChildren() )
                {
                    empty = true;
                }
                else
                {
                    const uint32_t cc = t->childToCont[pz.Child()];
                    c->idLo = std::max<int64_t>( c->idLo, t->conts[cc].base );
                    c->idHi = std::min<int64_t>( c->idHi, int64_t( t->conts[cc+1].base ) - 1 );
                }
            }
            break;
        }
    }
    if( empty || c->idLo > c->idHi || c->tsLo > c->tsHi || c->depthLo > c->depthHi || t->total == 0 ) return SQLITE_OK;

    // Threads to visit, in utid order, as id ranges.
    const auto ThreadRange = [&]( size_t u ) {
        return std::make_pair( t->conts[t->threadFirstCont[u]].base, t->conts[t->threadFirstCont[u+1]].base );
    };
    const auto Clip = [&]( uint32_t a, uint32_t b ) {
        const int64_t lo = std::max<int64_t>( a, c->idLo );
        const int64_t hi = std::min<int64_t>( b, c->idHi + 1 );
        if( lo < hi ) c->ranges.emplace_back( uint32_t( lo ), uint32_t( hi ) );
    };

    switch( c->mode )
    {
    case ModeRange:
    {
        const bool threadOrdered = ( idxNum & FlagThreadOrdered ) != 0;
        if( !c->useThreadMask && c->depthHi > 0 && !rootsFirst && !threadOrdered )
        {
            Clip( 0, uint32_t( t->total ) );
        }
        else
        {
            const bool rootsOnly = c->depthHi == 0;
            std::vector<size_t> order( nThreads );
            for( size_t u = 0; u < nThreads; u++ ) order[u] = u;
            if( threadOrdered )
            {
                const char* key = strchr( s, '|' );
                key = key ? key + 1 : "";
                const uint64_t pid = t->W().GetPid();
                std::sort( order.begin(), order.end(), [t, key, pid]( size_t a, size_t b ) {
                    const auto& ta = t->threads[a];
                    const auto& tb = t->threads[b];
                    for( const char* k = key; k[0]; k += 2 )
                    {
                        int cmp = 0;
                        switch( k[0] - 'A' )
                        {
                        case SC::ThreadName: cmp = strcmp( ta.name, tb.name ); break;
                        case SC::Tid: cmp = int64_t( ta.tid ) < int64_t( tb.tid ) ? -1 : int64_t( ta.tid ) > int64_t( tb.tid ) ? 1 : 0; break;
                        case SC::IsFiber: cmp = int( ta.isFiber ) - int( tb.isFiber ); break;
                        case SC::IsMainThread: cmp = int( pid != 0 && ta.tid == pid ) - int( pid != 0 && tb.tid == pid ); break;
                        default: cmp = a < b ? -1 : a > b ? 1 : 0; break;
                        }
                        if( cmp != 0 ) return k[1] == 'd' ? cmp > 0 : cmp < 0;
                    }
                    return a < b;
                } );
            }
            for( size_t u : order )
            {
                if( !c->ThreadOk( uint16_t( u ) ) ) continue;
                const auto r = ThreadRange( u );
                const uint32_t rootEnd = t->conts[t->threadFirstCont[u] + 1].base;
                if( rootsOnly || rootsFirst ) Clip( r.first, rootEnd );
                else Clip( r.first, r.second );
            }
            if( rootsFirst && !rootsOnly )
            {
                for( size_t u = 0; u < nThreads; u++ )
                {
                    if( !c->ThreadOk( uint16_t( u ) ) ) continue;
                    Clip( t->conts[t->threadFirstCont[u] + 1].base, ThreadRange( u ).second );
                }
            }
        }
        if( c->ranges.empty() ) return SQLITE_OK;
        c->ri = 0;
        c->rangeEnd = c->ranges[0].second;
        c->eof = !c->RangeSeek( c->ranges[0].first );
        break;
    }
    case ModeSrcEq:
    case ModeSrcOrdered:
    {
        t->BuildSrcIndex();
        for( auto u : t->usedSrc ) if( !c->useSrcSet || c->srcSet[u] ) c->srcList.push_back( u );
        if( c->mode == ModeSrcOrdered )
        {
            const char* key = strchr( s, '|' );
            key = key ? key + 1 : "";
            std::sort( c->srcList.begin(), c->srcList.end(), [t, key]( uint16_t a, uint16_t b ) {
                const auto& sa = t->src[a];
                const auto& sb = t->src[b];
                for( const char* k = key; k[0]; k += 2 )
                {
                    int cmp = 0;
                    switch( k[0] - 'A' )
                    {
                    case SC::Name: cmp = strcmp( sa.name, sb.name ); break;
                    case SC::File: cmp = strcmp( sa.file, sb.file ); break;
                    case SC::Function: cmp = strcmp( sa.function, sb.function ); break;
                    case SC::Line: cmp = sa.line < sb.line ? -1 : sa.line > sb.line ? 1 : 0; break;
                    case SC::SrcLoc: cmp = int( int16_t( a ) ) - int( int16_t( b ) ); break;
                    }
                    if( cmp != 0 ) return k[1] == 'd' ? cmp > 0 : cmp < 0;
                }
                return int16_t( a ) < int16_t( b );
            } );
        }
        // With a single thread the thread filter is an id range.
        if( c->useThreadMask )
        {
            int only = -1, n = 0;
            for( size_t u = 0; u < nThreads; u++ ) if( c->threadMask[u] ) { only = int( u ); n++; }
            if( n == 0 ) return SQLITE_OK;
            if( n == 1 )
            {
                const auto r = ThreadRange( size_t( only ) );
                c->idLo = std::max<int64_t>( c->idLo, r.first );
                c->idHi = std::min<int64_t>( c->idHi, int64_t( r.second ) - 1 );
                if( c->idLo > c->idHi ) return SQLITE_OK;
                c->useThreadMask = false;
            }
        }
        c->eof = false;
        c->SrcNext();
        break;
    }
    case ModeDfs:
    {
        for( size_t u = 0; u < nThreads; u++ )
        {
            if( !c->ThreadOk( uint16_t( u ) ) ) continue;
            const auto r = ThreadRange( u );
            if( int64_t( r.second ) <= c->idLo || int64_t( r.first ) > c->idHi ) continue;
            c->dfsThreads.push_back( uint32_t( u ) );
        }
        c->eof = false;
        c->DfsNext();
        break;
    }
    }
    return SQLITE_OK;
}

static int SliceNext( sqlite3_vtab_cursor* cur )
{
    auto c = static_cast<SliceCursor*>( cur );
    switch( c->mode )
    {
    case ModeRange: c->RangeNext(); break;
    case ModeDfs: c->DfsNext(); break;
    default: c->SrcNext(); break;
    }
    return SQLITE_OK;
}

static int SliceEof( sqlite3_vtab_cursor* cur )
{
    return static_cast<SliceCursor*>( cur )->eof;
}

static int SliceColumn( sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int col )
{
    auto c = static_cast<SliceCursor*>( cur );
    auto t = c->t;
    const ZoneEvent& z = *c->z;
    const Cont& cn = t->conts[c->cont];
    switch( col )
    {
    case SC::Id:
    case SC::SliceId:
        sqlite3_result_int64( ctx, c->id );
        break;
    case SC::Ts:
        sqlite3_result_int64( ctx, z.Start() );
        break;
    case SC::Dur:
        sqlite3_result_int64( ctx, z.IsEndValid() ? z.End() - z.Start() : -1 );
        break;
    case SC::TrackId:
    case SC::Utid:
        sqlite3_result_int( ctx, cn.utid );
        break;
    case SC::Name:
        ResultText( ctx, t->src[uint16_t( z.SrcLoc() )].name );
        break;
    case SC::Depth:
        sqlite3_result_int( ctx, cn.depth );
        break;
    case SC::ParentId:
        if( cn.parent == NoParent ) sqlite3_result_null( ctx );
        else sqlite3_result_int64( ctx, cn.parent );
        break;
    case SC::SrcLoc:
        sqlite3_result_int( ctx, z.SrcLoc() );
        break;
    case SC::File:
        ResultText( ctx, t->src[uint16_t( z.SrcLoc() )].file );
        break;
    case SC::Line:
        sqlite3_result_int64( ctx, t->src[uint16_t( z.SrcLoc() )].line );
        break;
    case SC::Function:
        ResultText( ctx, t->src[uint16_t( z.SrcLoc() )].function );
        break;
    case SC::ZoneName:
        if( t->W().HasZoneExtra( z ) && t->W().GetZoneExtra( z ).name.Active() ) ResultText( ctx, t->W().GetString( t->W().GetZoneExtra( z ).name ) );
        else sqlite3_result_null( ctx );
        break;
    case SC::Text:
        if( t->W().HasZoneExtra( z ) && t->W().GetZoneExtra( z ).text.Active() ) ResultText( ctx, t->W().GetString( t->W().GetZoneExtra( z ).text ) );
        else sqlite3_result_null( ctx );
        break;
    case SC::Color:
    {
        uint32_t color = t->src[uint16_t( z.SrcLoc() )].color;
        if( t->W().HasZoneExtra( z ) && t->W().GetZoneExtra( z ).color.Val() != 0 ) color = t->W().GetZoneExtra( z ).color.Val();
        if( color ) sqlite3_result_int64( ctx, color );
        else sqlite3_result_null( ctx );
        break;
    }
    case SC::ThreadName:
        ResultText( ctx, t->threads[cn.utid].name );
        break;
    case SC::Tid:
        sqlite3_result_int64( ctx, int64_t( t->threads[cn.utid].tid ) );
        break;
    case SC::IsMainThread:
        sqlite3_result_int( ctx, t->W().GetPid() != 0 && t->threads[cn.utid].tid == t->W().GetPid() );
        break;
    case SC::IsFiber:
        sqlite3_result_int( ctx, t->threads[cn.utid].isFiber );
        break;
    default:
        sqlite3_result_null( ctx );
        break;
    }
    return SQLITE_OK;
}

static int SliceRowid( sqlite3_vtab_cursor* cur, sqlite3_int64* rowid )
{
    *rowid = static_cast<SliceCursor*>( cur )->id;
    return SQLITE_OK;
}

static sqlite3_module SliceModule = {
    2, SliceConnect, SliceConnect, SliceBestIndex, SliceDisconnect, SliceDisconnect,
    SliceOpen, SliceClose, SliceFilter, SliceNext, SliceEof, SliceColumn, SliceRowid,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};

// ---------------------------------------------------------------------------
// Generic row-indexed tables
// ---------------------------------------------------------------------------

struct TableDef
{
    std::string name;
    std::string schema;
    std::function<int64_t()> rows;
    std::function<void( sqlite3_context*, int64_t, int )> column;
    int idCol = -1;         // column equal to the row index
    int partCol = -1;       // column whose equality selects a contiguous row range
    std::function<std::pair<int64_t, int64_t>( int64_t )> partRange;
};

struct SimpleVtab : sqlite3_vtab
{
    TableDef* def;
};

struct SimpleCursor : sqlite3_vtab_cursor
{
    TableDef* def;
    int64_t row = 0, end = 0;
};

static int SimpleConnect( sqlite3* db, void* aux, int, const char* const*, sqlite3_vtab** out, char** )
{
    auto def = (TableDef*)aux;
    int rc = sqlite3_declare_vtab( db, def->schema.c_str() );
    if( rc != SQLITE_OK ) return rc;
    sqlite3_vtab_config( db, SQLITE_VTAB_INNOCUOUS );
    auto v = new SimpleVtab();
    memset( static_cast<sqlite3_vtab*>( v ), 0, sizeof( sqlite3_vtab ) );
    v->def = def;
    *out = v;
    return SQLITE_OK;
}

static int SimpleDisconnect( sqlite3_vtab* v )
{
    delete static_cast<SimpleVtab*>( v );
    return SQLITE_OK;
}

static int SimpleBestIndex( sqlite3_vtab* vt, sqlite3_index_info* info )
{
    auto def = static_cast<SimpleVtab*>( vt )->def;
    const double n = double( std::max<int64_t>( def->rows(), 1 ) );
    int idC = -1, partC = -1;
    for( int i = 0; i < info->nConstraint; i++ )
    {
        const auto& c = info->aConstraint[i];
        if( !c.usable || c.op != SQLITE_INDEX_CONSTRAINT_EQ ) continue;
        if( def->idCol >= 0 && ( c.iColumn == def->idCol || c.iColumn < 0 ) ) idC = i;
        else if( def->partCol >= 0 && c.iColumn == def->partCol ) partC = i;
    }
    if( idC >= 0 )
    {
        info->aConstraintUsage[idC].argvIndex = 1;
        info->idxNum = 1;
        info->estimatedRows = 1;
        info->estimatedCost = 1;
        info->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
    }
    else if( partC >= 0 )
    {
        info->aConstraintUsage[partC].argvIndex = 1;
        info->idxNum = 2;
        info->estimatedRows = sqlite3_int64( n / 10 ) + 1;
        info->estimatedCost = n / 10 + 5;
    }
    else
    {
        info->idxNum = 0;
        info->estimatedRows = sqlite3_int64( n );
        info->estimatedCost = n + 10;
    }
    // Rows come out by ascending id, which is also ascending in the partition column.
    const auto IsId = [def]( int c ) { return c < 0 || ( def->idCol >= 0 && c == def->idCol ); };
    const auto IsPart = [def]( int c ) { return def->partCol >= 0 && c == def->partCol; };
    const auto ob = info->aOrderBy;
    if( ( info->nOrderBy == 1 && !ob[0].desc && ( IsId( ob[0].iColumn ) || IsPart( ob[0].iColumn ) ) ) ||
        ( info->nOrderBy == 2 && !ob[0].desc && !ob[1].desc && IsPart( ob[0].iColumn ) && IsId( ob[1].iColumn ) ) )
    {
        info->orderByConsumed = 1;
    }
    return SQLITE_OK;
}

static int SimpleOpen( sqlite3_vtab* vt, sqlite3_vtab_cursor** out )
{
    auto c = new SimpleCursor();
    memset( static_cast<sqlite3_vtab_cursor*>( c ), 0, sizeof( sqlite3_vtab_cursor ) );
    c->def = static_cast<SimpleVtab*>( vt )->def;
    *out = c;
    return SQLITE_OK;
}

static int SimpleClose( sqlite3_vtab_cursor* c )
{
    delete static_cast<SimpleCursor*>( c );
    return SQLITE_OK;
}

static int SimpleFilter( sqlite3_vtab_cursor* cur, int idxNum, const char*, int argc, sqlite3_value** argv )
{
    auto c = static_cast<SimpleCursor*>( cur );
    const int64_t n = c->def->rows();
    c->row = 0;
    c->end = n;
    if( idxNum != 0 && argc == 1 )
    {
        int64_t v;
        if( GetIntArg( argv[0], 'e', v ) != IntVal::Ok )
        {
            c->end = 0;
        }
        else if( idxNum == 1 )
        {
            if( v < 0 || v >= n ) c->end = 0;
            else { c->row = v; c->end = v + 1; }
        }
        else
        {
            const auto r = c->def->partRange( v );
            c->row = r.first;
            c->end = r.second;
        }
    }
    return SQLITE_OK;
}

static int SimpleNext( sqlite3_vtab_cursor* cur )
{
    static_cast<SimpleCursor*>( cur )->row++;
    return SQLITE_OK;
}

static int SimpleEof( sqlite3_vtab_cursor* cur )
{
    auto c = static_cast<SimpleCursor*>( cur );
    return c->row >= c->end;
}

static int SimpleColumn( sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int col )
{
    auto c = static_cast<SimpleCursor*>( cur );
    c->def->column( ctx, c->row, col );
    return SQLITE_OK;
}

static int SimpleRowid( sqlite3_vtab_cursor* cur, sqlite3_int64* rowid )
{
    *rowid = static_cast<SimpleCursor*>( cur )->row;
    return SQLITE_OK;
}

static sqlite3_module SimpleModule = {
    2, SimpleConnect, SimpleConnect, SimpleBestIndex, SimpleDisconnect, SimpleDisconnect,
    SimpleOpen, SimpleClose, SimpleFilter, SimpleNext, SimpleEof, SimpleColumn, SimpleRowid,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};

// Prefix sums over groups of rows (plots, frame sets, locks, memory pools).
struct Groups
{
    std::vector<int64_t> base { 0 };
    void Add( int64_t n ) { base.push_back( base.back() + n ); }
    int64_t Total() const { return base.back(); }
    size_t Count() const { return base.size() - 1; }
    size_t Find( int64_t row ) const { return size_t( std::upper_bound( base.begin(), base.end(), row ) - base.begin() ) - 1; }
    std::pair<int64_t, int64_t> Range( int64_t g ) const
    {
        if( g < 0 || g >= int64_t( Count() ) ) return { 0, 0 };
        return { base[g], base[g+1] };
    }
};

static int Exec( sqlite3* db, const std::string& sql, std::string& err )
{
    char* msg = nullptr;
    int rc = sqlite3_exec( db, sql.c_str(), nullptr, nullptr, &msg );
    if( rc != SQLITE_OK )
    {
        err = msg ? msg : "sqlite error";
        sqlite3_free( msg );
    }
    return rc;
}

static bool AddTable( TraceDb* t, TableDef* def, std::string& err )
{
    const std::string mod = "tracy_" + def->name;
    if( sqlite3_create_module_v2( t->db, mod.c_str(), &SimpleModule, def, []( void* p ) { delete (TableDef*)p; } ) != SQLITE_OK )
    {
        err = sqlite3_errmsg( t->db );
        return false;
    }
    return Exec( t->db, "CREATE VIRTUAL TABLE " + def->name + " USING " + mod, err ) == SQLITE_OK;
}

static std::string Quote( const std::string& s )
{
    std::string r = "'";
    for( char c : s ) { if( c == '\'' ) r += "''"; else r.push_back( c ); }
    r.push_back( '\'' );
    return r;
}

// ---------------------------------------------------------------------------
// Materialised helpers for the small tables
// ---------------------------------------------------------------------------

struct GpuRow
{
    const GpuEvent* ev;
    int64_t parent;
    int depth;
    int ctx;
};

static void WalkGpu( const Worker& w, const Vector<short_ptr<GpuEvent>>& v, int64_t parent, int depth, int ctx, std::vector<GpuRow>& out )
{
    const size_t sz = v.size();
    for( size_t i = 0; i < sz; i++ )
    {
        const GpuEvent* ev = v.is_magic() ? &( *(const Vector<GpuEvent>*)&v )[i] : (const GpuEvent*)v[i].get();
        const int64_t id = int64_t( out.size() );
        out.push_back( GpuRow { ev, parent, depth, ctx } );
        if( ev->Child() >= 0 ) WalkGpu( w, w.GetGpuChildren( ev->Child() ), id, depth + 1, ctx, out );
    }
}

struct LockWaitRow
{
    uint32_t lock;
    uint64_t tid;
    int64_t ts, dur;
    bool shared;
};

struct MetaRow
{
    std::string name;
    bool isInt;
    int64_t i;
    std::string s;
};

// ---------------------------------------------------------------------------
// SQL functions
// ---------------------------------------------------------------------------

static void FnTraceStart( sqlite3_context* ctx, int, sqlite3_value** )
{
    auto t = (TraceDb*)sqlite3_user_data( ctx );
    sqlite3_result_int64( ctx, t->W().GetFirstTime() );
}

static void FnTraceEnd( sqlite3_context* ctx, int, sqlite3_value** )
{
    auto t = (TraceDb*)sqlite3_user_data( ctx );
    sqlite3_result_int64( ctx, t->W().GetLastTime() );
}

static void FnTraceDur( sqlite3_context* ctx, int, sqlite3_value** )
{
    auto t = (TraceDb*)sqlite3_user_data( ctx );
    sqlite3_result_int64( ctx, t->W().GetLastTime() - t->W().GetFirstTime() );
}

// regexp( pattern, text [, flags] ), also backing the REGEXP operator. ECMAScript syntax, unanchored.
static void FnRegexp( sqlite3_context* ctx, int argc, sqlite3_value** argv )
{
    if( sqlite3_value_type( argv[0] ) == SQLITE_NULL || sqlite3_value_type( argv[1] ) == SQLITE_NULL )
    {
        sqlite3_result_null( ctx );
        return;
    }
    struct Cache { std::string key; std::regex re; };
    static Cache cache;
    const char* pat = (const char*)sqlite3_value_text( argv[0] );
    const char* txt = (const char*)sqlite3_value_text( argv[1] );
    const char* flags = argc > 2 && sqlite3_value_type( argv[2] ) != SQLITE_NULL ? (const char*)sqlite3_value_text( argv[2] ) : "";
    std::string key = std::string( flags ) + '\n' + pat;
    if( cache.key != key )
    {
        try
        {
            auto f = std::regex::ECMAScript | std::regex::optimize;
            if( strchr( flags, 'i' ) ) f |= std::regex::icase;
            cache.re = std::regex( pat, f );
            cache.key = key;
        }
        catch( const std::regex_error& e )
        {
            sqlite3_result_error( ctx, ( std::string( "regexp: invalid pattern: " ) + e.what() ).c_str(), -1 );
            return;
        }
    }
    sqlite3_result_int( ctx, std::regex_search( txt, cache.re ) );
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

static bool RegisterTables( TraceDb* t, std::string& err )
{
    const Worker& w = t->W();
    sqlite3* db = t->db;

    sqlite3_create_function( db, "trace_start", 0, SQLITE_UTF8 | SQLITE_DETERMINISTIC, t, FnTraceStart, nullptr, nullptr );
    sqlite3_create_function( db, "trace_end", 0, SQLITE_UTF8 | SQLITE_DETERMINISTIC, t, FnTraceEnd, nullptr, nullptr );
    sqlite3_create_function( db, "trace_dur", 0, SQLITE_UTF8 | SQLITE_DETERMINISTIC, t, FnTraceDur, nullptr, nullptr );
    sqlite3_create_function( db, "regexp", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, t, FnRegexp, nullptr, nullptr );
    sqlite3_create_function( db, "regexp", 3, SQLITE_UTF8 | SQLITE_DETERMINISTIC, t, FnRegexp, nullptr, nullptr );

    // slice
    if( sqlite3_create_module_v2( db, "tracy_slice", &SliceModule, t, nullptr ) != SQLITE_OK ) { err = sqlite3_errmsg( db ); return false; }
    if( Exec( db, "CREATE VIRTUAL TABLE slice USING tracy_slice", err ) != SQLITE_OK ) return false;

    const uint64_t pid = w.GetPid();
    const int nThreads = int( t->threads.size() );

    // thread
    {
        auto d = new TableDef();
        d->name = "thread";
        d->schema = "CREATE TABLE x(utid INTEGER, id INTEGER, tid INTEGER, name TEXT, start_ts INTEGER, end_ts INTEGER, upid INTEGER, "
                    "is_main_thread INTEGER, is_idle INTEGER, machine_id INTEGER, arg_set_id INTEGER, is_fiber INTEGER, zone_count INTEGER)";
        d->idCol = 0;
        d->rows = [t] { return int64_t( t->threads.size() ); };
        d->column = [t, pid]( sqlite3_context* ctx, int64_t r, int col ) {
            const auto& th = t->threads[r];
            switch( col )
            {
            case 0: case 1: sqlite3_result_int64( ctx, r ); break;
            case 2: sqlite3_result_int64( ctx, int64_t( th.tid ) ); break;
            case 3: ResultText( ctx, th.name ); break;
            case 4: if( th.hasTime ) sqlite3_result_int64( ctx, th.start ); else sqlite3_result_null( ctx ); break;
            case 5: if( th.hasTime ) sqlite3_result_int64( ctx, th.end ); else sqlite3_result_null( ctx ); break;
            case 6: sqlite3_result_int( ctx, 0 ); break;
            case 7: sqlite3_result_int( ctx, pid != 0 && th.tid == pid ); break;
            case 8: sqlite3_result_int( ctx, 0 ); break;
            case 9: sqlite3_result_int( ctx, 0 ); break;
            case 11: sqlite3_result_int( ctx, th.isFiber ); break;
            case 12: sqlite3_result_int64( ctx, int64_t( th.count ) ); break;
            default: sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // process
    {
        auto d = new TableDef();
        d->name = "process";
        d->schema = "CREATE TABLE x(upid INTEGER, id INTEGER, pid INTEGER, name TEXT, start_ts INTEGER, end_ts INTEGER, parent_upid INTEGER, "
                    "uid INTEGER, android_appid INTEGER, android_user_id INTEGER, cmdline TEXT, arg_set_id INTEGER, machine_id INTEGER)";
        d->idCol = 0;
        d->rows = [] { return int64_t( 1 ); };
        d->column = [t, pid]( sqlite3_context* ctx, int64_t, int col ) {
            switch( col )
            {
            case 0: case 1: case 12: sqlite3_result_int( ctx, 0 ); break;
            case 2: sqlite3_result_int64( ctx, int64_t( pid ) ); break;
            case 3: sqlite3_result_text( ctx, t->W().GetCaptureProgram().c_str(), -1, SQLITE_TRANSIENT ); break;
            default: sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // thread_track: one per thread, id == utid
    {
        auto d = new TableDef();
        d->name = "thread_track";
        d->schema = "CREATE TABLE x(id INTEGER, name TEXT, type TEXT, parent_id INTEGER, source_arg_set_id INTEGER, machine_id INTEGER, utid INTEGER)";
        d->idCol = 0;
        d->rows = [t] { return int64_t( t->threads.size() ); };
        d->column = []( sqlite3_context* ctx, int64_t r, int col ) {
            switch( col )
            {
            case 0: case 6: sqlite3_result_int64( ctx, r ); break;
            case 2: ResultText( ctx, "thread_execution" ); break;
            case 5: sqlite3_result_int( ctx, 0 ); break;
            default: sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // counter_track / counter (plots). Track ids follow the thread tracks.
    auto plots = std::make_shared<std::vector<const PlotData*>>();
    auto plotGroups = std::make_shared<Groups>();
    for( const auto* pd : w.GetPlots() )
    {
        if( !pd ) continue;
        plots->push_back( pd );
        plotGroups->Add( int64_t( pd->data.size() ) );
    }
    {
        auto d = new TableDef();
        d->name = "counter_track";
        d->schema = "CREATE TABLE x(id INTEGER, name TEXT, parent_id INTEGER, type TEXT, dimension_arg_set_id INTEGER, source_arg_set_id INTEGER, "
                    "machine_id INTEGER, unit TEXT, description TEXT, plot_type TEXT, format TEXT, min REAL, max REAL, count INTEGER)";
        d->rows = [plots] { return int64_t( plots->size() ); };
        d->partCol = 0;
        d->partRange = [plots, nThreads]( int64_t v ) {
            v -= nThreads;
            if( v < 0 || v >= int64_t( plots->size() ) ) return std::make_pair<int64_t, int64_t>( 0, 0 );
            return std::make_pair( v, v + 1 );
        };
        d->column = [t, plots, nThreads]( sqlite3_context* ctx, int64_t r, int col ) {
            static const char* types[] = { "User", "Memory", "SysTime", "Power" };
            static const char* formats[] = { "Number", "Memory", "Percentage", "Watt" };
            static const char* units[] = { nullptr, "bytes", "%", "W" };
            const auto* pd = ( *plots )[r];
            const uint8_t ty = uint8_t( pd->type );
            const uint8_t fm = uint8_t( pd->format );
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, nThreads + r ); break;
            case 1: ResultText( ctx, t->W().GetString( pd->name ) ); break;
            case 3: ResultText( ctx, "tracy_plot" ); break;
            case 6: sqlite3_result_int( ctx, 0 ); break;
            case 7: ResultText( ctx, fm < 4 ? units[fm] : nullptr ); break;
            case 9: ResultText( ctx, ty < 4 ? types[ty] : "Unknown" ); break;
            case 10: ResultText( ctx, fm < 4 ? formats[fm] : "Unknown" ); break;
            case 11: if( pd->data.empty() ) sqlite3_result_null( ctx ); else sqlite3_result_double( ctx, pd->min ); break;
            case 12: if( pd->data.empty() ) sqlite3_result_null( ctx ); else sqlite3_result_double( ctx, pd->max ); break;
            case 13: sqlite3_result_int64( ctx, int64_t( pd->data.size() ) ); break;
            default: sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }
    {
        auto d = new TableDef();
        d->name = "counter";
        d->schema = "CREATE TABLE x(id INTEGER, ts INTEGER, track_id INTEGER, value REAL, arg_set_id INTEGER)";
        d->idCol = 0;
        d->partCol = 2;
        d->rows = [plotGroups] { return plotGroups->Total(); };
        d->partRange = [plotGroups, nThreads]( int64_t v ) { return plotGroups->Range( v - nThreads ); };
        d->column = [plots, plotGroups, nThreads]( sqlite3_context* ctx, int64_t r, int col ) {
            const size_t g = plotGroups->Find( r );
            const auto& item = ( *plots )[g]->data[size_t( r - plotGroups->base[g] )];
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, r ); break;
            case 1: sqlite3_result_int64( ctx, item.time.Val() ); break;
            case 2: sqlite3_result_int64( ctx, nThreads + int64_t( g ) ); break;
            case 3: sqlite3_result_double( ctx, item.val ); break;
            default: sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // source_location
    {
        auto d = new TableDef();
        d->name = "source_location";
        d->schema = "CREATE TABLE x(id INTEGER, name TEXT, function TEXT, file TEXT, line INTEGER, color INTEGER, zone_count INTEGER)";
        d->rows = [t] { t->BuildIndex(); return int64_t( t->allSrc.size() ); };
        d->column = [t]( sqlite3_context* ctx, int64_t r, int col ) {
            const int16_t id = t->allSrc[r];
            const auto& s = t->src[uint16_t( id )];
            switch( col )
            {
            case 0: sqlite3_result_int( ctx, id ); break;
            case 1: ResultText( ctx, s.name ); break;
            case 2: ResultText( ctx, s.function ); break;
            case 3: ResultText( ctx, s.file ); break;
            case 4: sqlite3_result_int64( ctx, s.line ); break;
            case 5: if( s.color ) sqlite3_result_int64( ctx, s.color ); else sqlite3_result_null( ctx ); break;
            case 6: sqlite3_result_int64( ctx, int64_t( t->srclocCount[uint16_t( id )] ) ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // frame
    {
        auto sets = std::make_shared<std::vector<const FrameData*>>();
        auto groups = std::make_shared<Groups>();
        for( const auto* fd : w.GetFrames() )
        {
            if( !fd ) continue;
            sets->push_back( fd );
            groups->Add( int64_t( w.GetFrameCount( *fd ) ) );
        }
        auto d = new TableDef();
        d->name = "frame";
        d->schema = "CREATE TABLE x(id INTEGER, frame_set_id INTEGER, name TEXT, idx INTEGER, ts INTEGER, dur INTEGER, continuous INTEGER)";
        d->idCol = 0;
        d->partCol = 1;
        d->rows = [groups] { return groups->Total(); };
        d->partRange = [groups]( int64_t v ) { return groups->Range( v ); };
        d->column = [t, sets, groups]( sqlite3_context* ctx, int64_t r, int col ) {
            const size_t g = groups->Find( r );
            const auto* fd = ( *sets )[g];
            const size_t i = size_t( r - groups->base[g] );
            const auto& w = t->W();
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, r ); break;
            case 1: sqlite3_result_int64( ctx, int64_t( g ) ); break;
            case 2: ResultText( ctx, fd->name == 0 ? "Frame" : w.GetString( fd->name ) ); break;
            case 3: sqlite3_result_int64( ctx, int64_t( i ) ); break;
            case 4: sqlite3_result_int64( ctx, w.GetFrameBegin( *fd, i ) ); break;
            case 5: sqlite3_result_int64( ctx, w.GetFrameEnd( *fd, i ) - w.GetFrameBegin( *fd, i ) ); break;
            case 6: sqlite3_result_int( ctx, fd->continuous ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // gpu_slice
    {
        auto rows = std::make_shared<std::vector<GpuRow>>();
        auto ready = std::make_shared<bool>( false );
        auto ensure = [t, rows, ready] {
            if( *ready ) return;
            *ready = true;
            const auto& w = t->W();
            int ci = 0;
            for( const auto* ctx : w.GetGpuData() )
            {
                if( ctx )
                {
                    std::vector<uint64_t> tids;
                    for( const auto& kv : ctx->threadData ) tids.push_back( kv.first );
                    std::sort( tids.begin(), tids.end() );
                    for( auto tid : tids ) WalkGpu( w, ctx->threadData.find( tid )->second.timeline, -1, 0, ci, *rows );
                }
                ci++;
            }
        };
        auto d = new TableDef();
        d->name = "gpu_slice";
        d->schema = "CREATE TABLE x(id INTEGER, ts INTEGER, dur INTEGER, name TEXT, depth INTEGER, parent_id INTEGER, context_id INTEGER, "
                    "context_name TEXT, context_type TEXT, utid INTEGER, cpu_ts INTEGER, cpu_dur INTEGER, source_location_id INTEGER, "
                    "file TEXT, line INTEGER, function TEXT)";
        d->idCol = 0;
        d->rows = [rows, ensure] { ensure(); return int64_t( rows->size() ); };
        d->column = [t, rows]( sqlite3_context* ctx, int64_t r, int col ) {
            static const char* types[] = { "Invalid", "OpenGL", "Vulkan", "OpenCL", "Direct3D12", "Direct3D11", "Metal", "Custom", "CUDA", "Rocprof", "WebGPU" };
            const auto& row = ( *rows )[r];
            const auto& ev = *row.ev;
            const auto& w = t->W();
            const auto* gc = w.GetGpuData()[row.ctx];
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, r ); break;
            case 1: sqlite3_result_int64( ctx, ev.GpuStart() ); break;
            case 2: sqlite3_result_int64( ctx, ev.GpuEnd() >= 0 ? ev.GpuEnd() - ev.GpuStart() : -1 ); break;
            case 3: ResultText( ctx, w.GetZoneName( w.GetSourceLocation( ev.SrcLoc() ) ) ); break;
            case 4: sqlite3_result_int( ctx, row.depth ); break;
            case 5: if( row.parent >= 0 ) sqlite3_result_int64( ctx, row.parent ); else sqlite3_result_null( ctx ); break;
            case 6: sqlite3_result_int( ctx, row.ctx ); break;
            case 7: if( gc->name.Active() ) ResultText( ctx, w.GetString( gc->name ) ); else sqlite3_result_null( ctx ); break;
            case 8: ResultText( ctx, uint8_t( gc->type ) < 11 ? types[uint8_t( gc->type )] : "Unknown" ); break;
            case 9: ResultUtid( ctx, t->UtidOfCompressed( ev.Thread() ) ); break;
            case 10: sqlite3_result_int64( ctx, ev.CpuStart() ); break;
            case 11: sqlite3_result_int64( ctx, ev.CpuEnd() >= 0 ? ev.CpuEnd() - ev.CpuStart() : -1 ); break;
            case 12: sqlite3_result_int( ctx, ev.SrcLoc() ); break;
            case 13: ResultText( ctx, w.GetString( w.GetSourceLocation( ev.SrcLoc() ).file ) ); break;
            case 14: sqlite3_result_int64( ctx, w.GetSourceLocation( ev.SrcLoc() ).line ); break;
            case 15: ResultText( ctx, w.GetString( w.GetSourceLocation( ev.SrcLoc() ).function ) ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // message
    {
        auto d = new TableDef();
        d->name = "message";
        d->schema = "CREATE TABLE x(id INTEGER, ts INTEGER, utid INTEGER, text TEXT, color INTEGER, severity TEXT, source TEXT)";
        d->idCol = 0;
        d->rows = [t] { return int64_t( t->W().GetMessages().size() ); };
        d->column = [t]( sqlite3_context* ctx, int64_t r, int col ) {
            static const char* sev[] = { "Trace", "Debug", "Info", "Warning", "Error", "Fatal" };
            const MessageData& m = *t->W().GetMessages()[r];
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, r ); break;
            case 1: sqlite3_result_int64( ctx, m.time ); break;
            case 2: ResultUtid( ctx, t->UtidOfCompressed( m.thread ) ); break;
            case 3: ResultText( ctx, t->W().GetString( m.ref ) ); break;
            case 4: if( m.color ) sqlite3_result_int64( ctx, m.color ); else sqlite3_result_null( ctx ); break;
            case 5: ResultText( ctx, uint8_t( m.severity ) < 6 ? sev[uint8_t( m.severity )] : "Unknown" ); break;
            case 6: ResultText( ctx, m.source == MessageSourceType::User ? "User" : "Tracy" ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // lock, lock_event, lock_wait
    {
        auto locks = std::make_shared<std::vector<std::pair<uint32_t, const LockMap*>>>();
        for( const auto& kv : w.GetLockMap() ) if( kv.second && kv.second->valid ) locks->emplace_back( kv.first, kv.second );
        std::sort( locks->begin(), locks->end(), []( const auto& a, const auto& b ) { return a.first < b.first; } );
        auto groups = std::make_shared<Groups>();
        for( const auto& l : *locks ) groups->Add( int64_t( l.second->timeline.size() ) );
        auto lockName = [t]( const LockMap* lm ) {
            return lm->customName.Active() ? t->W().GetString( lm->customName ) : t->W().GetZoneName( t->W().GetSourceLocation( lm->srcloc ) );
        };
        auto lockRange = [locks]( int64_t id ) {
            auto it = std::lower_bound( locks->begin(), locks->end(), id, []( const auto& a, int64_t v ) { return int64_t( a.first ) < v; } );
            if( it == locks->end() || int64_t( it->first ) != id ) return int64_t( -1 );
            return int64_t( it - locks->begin() );
        };
        {
            auto d = new TableDef();
            d->name = "lock";
            d->schema = "CREATE TABLE x(id INTEGER, name TEXT, type TEXT, is_contended INTEGER, ts_announce INTEGER, ts_terminate INTEGER, "
                        "source_location_id INTEGER, file TEXT, line INTEGER, event_count INTEGER, thread_count INTEGER, wait_count INTEGER, wait_total INTEGER)";
            d->partCol = 0;
            d->rows = [locks] { return int64_t( locks->size() ); };
            d->partRange = [lockRange]( int64_t v ) {
                const int64_t i = lockRange( v );
                return i < 0 ? std::make_pair<int64_t, int64_t>( 0, 0 ) : std::make_pair( i, i + 1 );
            };
            d->column = [t, locks, lockName]( sqlite3_context* ctx, int64_t r, int col ) {
                const auto& l = ( *locks )[r];
                const auto* lm = l.second;
                const auto& sl = t->W().GetSourceLocation( lm->srcloc );
                switch( col )
                {
                case 0: sqlite3_result_int64( ctx, l.first ); break;
                case 1: ResultText( ctx, lockName( lm ) ); break;
                case 2: ResultText( ctx, lm->type == LockType::Lockable ? "Lockable" : "SharedLockable" ); break;
                case 3: sqlite3_result_int( ctx, lm->isContended ); break;
                case 4: sqlite3_result_int64( ctx, lm->timeAnnounce ); break;
                case 5: if( lm->timeTerminate > 0 ) sqlite3_result_int64( ctx, lm->timeTerminate ); else sqlite3_result_null( ctx ); break;
                case 6: sqlite3_result_int( ctx, lm->srcloc ); break;
                case 7: ResultText( ctx, t->W().GetString( sl.file ) ); break;
                case 8: sqlite3_result_int64( ctx, sl.line ); break;
                case 9: sqlite3_result_int64( ctx, int64_t( lm->timeline.size() ) ); break;
                case 10: sqlite3_result_int64( ctx, int64_t( lm->threads.size() ) ); break;
                case 11:
                case 12:
                {
                    int64_t cnt = 0, total = 0;
                    for( const auto& ti : lm->threads ) { cnt += int64_t( ti.waitCount ); total += ti.waitTotal; }
                    sqlite3_result_int64( ctx, col == 11 ? cnt : total );
                    break;
                }
                }
            };
            if( !AddTable( t, d, err ) ) return false;
        }
        {
            auto d = new TableDef();
            d->name = "lock_event";
            d->schema = "CREATE TABLE x(id INTEGER, lock_id INTEGER, ts INTEGER, type TEXT, utid INTEGER, tid INTEGER, source_location_id INTEGER)";
            d->idCol = 0;
            d->partCol = 1;
            d->rows = [groups] { return groups->Total(); };
            d->partRange = [groups, lockRange]( int64_t v ) { return groups->Range( lockRange( v ) ); };
            d->column = [t, locks, groups]( sqlite3_context* ctx, int64_t r, int col ) {
                static const char* types[] = { "Wait", "Obtain", "Release", "WaitShared", "ObtainShared", "ReleaseShared" };
                const size_t g = groups->Find( r );
                const auto* lm = ( *locks )[g].second;
                const auto& ev = lm->timeline[size_t( r - groups->base[g] )];
                const bool hasThread = ev.thread < lm->threads.size();
                const uint64_t tid = hasThread ? lm->threads[ev.thread].thread : 0;
                switch( col )
                {
                case 0: sqlite3_result_int64( ctx, r ); break;
                case 1: sqlite3_result_int64( ctx, ( *locks )[g].first ); break;
                case 2: sqlite3_result_int64( ctx, ev.Time() ); break;
                case 3: ResultText( ctx, ev.type < 6 ? types[ev.type] : "Unknown" ); break;
                case 4: ResultUtid( ctx, hasThread ? t->UtidOfTid( tid ) : -1 ); break;
                case 5: if( hasThread ) sqlite3_result_int64( ctx, int64_t( tid ) ); else sqlite3_result_null( ctx ); break;
                case 6: if( ev.SrcLoc() != 0 ) sqlite3_result_int( ctx, ev.SrcLoc() ); else sqlite3_result_null( ctx ); break;
                }
            };
            if( !AddTable( t, d, err ) ) return false;
        }
        {
            auto rows = std::make_shared<std::vector<LockWaitRow>>();
            auto ready = std::make_shared<bool>( false );
            auto ensure = [locks, rows, ready] {
                if( *ready ) return;
                *ready = true;
                for( const auto& l : *locks )
                {
                    const auto* lm = l.second;
                    std::vector<int64_t> open( lm->threads.size(), INT64_MIN );
                    for( const auto& ev : lm->timeline )
                    {
                        if( ev.thread >= open.size() ) continue;
                        const auto type = LockEvent::Type( ev.type );
                        if( type == LockEvent::Type::Wait || type == LockEvent::Type::WaitShared )
                        {
                            if( open[ev.thread] == INT64_MIN ) open[ev.thread] = ev.Time();
                        }
                        else if( type == LockEvent::Type::Obtain || type == LockEvent::Type::ObtainShared )
                        {
                            if( open[ev.thread] != INT64_MIN )
                            {
                                rows->push_back( LockWaitRow { l.first, lm->threads[ev.thread].thread, open[ev.thread], ev.Time() - open[ev.thread], type == LockEvent::Type::ObtainShared } );
                                open[ev.thread] = INT64_MIN;
                            }
                        }
                    }
                    for( size_t i = 0; i < open.size(); i++ )
                    {
                        if( open[i] != INT64_MIN ) rows->push_back( LockWaitRow { l.first, lm->threads[i].thread, open[i], -1, false } );
                    }
                }
            };
            auto d = new TableDef();
            d->name = "lock_wait";
            d->schema = "CREATE TABLE x(id INTEGER, lock_id INTEGER, utid INTEGER, tid INTEGER, ts INTEGER, dur INTEGER, shared INTEGER)";
            d->idCol = 0;
            d->rows = [rows, ensure] { ensure(); return int64_t( rows->size() ); };
            d->column = [t, rows]( sqlite3_context* ctx, int64_t r, int col ) {
                const auto& row = ( *rows )[r];
                switch( col )
                {
                case 0: sqlite3_result_int64( ctx, r ); break;
                case 1: sqlite3_result_int64( ctx, row.lock ); break;
                case 2: ResultUtid( ctx, t->UtidOfTid( row.tid ) ); break;
                case 3: sqlite3_result_int64( ctx, int64_t( row.tid ) ); break;
                case 4: sqlite3_result_int64( ctx, row.ts ); break;
                case 5: sqlite3_result_int64( ctx, row.dur ); break;
                case 6: sqlite3_result_int( ctx, row.shared ); break;
                }
            };
            if( !AddTable( t, d, err ) ) return false;
        }
    }

    // memory_event
    {
        auto pools = std::make_shared<std::vector<const MemData*>>();
        for( const auto& kv : w.GetMemNameMap() ) if( kv.second ) pools->push_back( kv.second );
        std::sort( pools->begin(), pools->end(), []( const MemData* a, const MemData* b ) { return a->name < b->name; } );
        auto groups = std::make_shared<Groups>();
        for( const auto* p : *pools ) groups->Add( int64_t( p->data.size() ) );
        auto d = new TableDef();
        d->name = "memory_event";
        d->schema = "CREATE TABLE x(id INTEGER, pool_id INTEGER, pool TEXT, ptr INTEGER, size INTEGER, ts INTEGER, dur INTEGER, ts_free INTEGER, "
                    "utid INTEGER, utid_free INTEGER, callstack_id INTEGER)";
        d->idCol = 0;
        d->partCol = 1;
        d->rows = [groups] { return groups->Total(); };
        d->partRange = [groups]( int64_t v ) { return groups->Range( v ); };
        d->column = [t, pools, groups]( sqlite3_context* ctx, int64_t r, int col ) {
            const size_t g = groups->Find( r );
            const auto* md = ( *pools )[g];
            const auto& ev = md->data[size_t( r - groups->base[g] )];
            const bool freed = ev.TimeFree() >= 0;
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, r ); break;
            case 1: sqlite3_result_int64( ctx, int64_t( g ) ); break;
            case 2: ResultText( ctx, md->name == 0 ? "(default)" : t->W().GetString( md->name ) ); break;
            case 3: sqlite3_result_int64( ctx, int64_t( ev.Ptr() ) ); break;
            case 4: sqlite3_result_int64( ctx, int64_t( ev.Size() ) ); break;
            case 5: sqlite3_result_int64( ctx, ev.TimeAlloc() ); break;
            case 6: sqlite3_result_int64( ctx, freed ? ev.TimeFree() - ev.TimeAlloc() : -1 ); break;
            case 7: if( freed ) sqlite3_result_int64( ctx, ev.TimeFree() ); else sqlite3_result_null( ctx ); break;
            case 8: ResultUtid( ctx, t->UtidOfCompressed( ev.ThreadAlloc() ) ); break;
            case 9: ResultUtid( ctx, freed ? t->UtidOfCompressed( ev.ThreadFree() ) : -1 ); break;
            case 10: if( ev.CsAlloc() ) sqlite3_result_int64( ctx, ev.CsAlloc() ); else sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // metadata
    {
        auto rows = std::make_shared<std::vector<MetaRow>>();
        const auto S = [&]( const char* n, const std::string& v ) { rows->push_back( MetaRow { n, false, 0, v } ); };
        const auto I = [&]( const char* n, int64_t v ) { rows->push_back( MetaRow { n, true, v, "" } ); };
        char ver[64];
        snprintf( ver, sizeof( ver ), "%i.%i.%i", Version::Major, Version::Minor, Version::Patch );
        const int fv = w.GetTraceVersion();
        char fver[64];
        snprintf( fver, sizeof( fver ), "%i.%i.%i", fv >> 16, ( fv >> 8 ) & 0xFF, fv & 0xFF );
        S( "trace_type", "tracy" );
        S( "trace_file", t->path );
        S( "tracy_version", ver );
        S( "tracy_file_version", fver );
        S( "capture_name", w.GetCaptureName() );
        S( "capture_program", w.GetCaptureProgram() );
        I( "capture_time", int64_t( w.GetCaptureTime() ) );
        I( "executable_time", int64_t( w.GetExecutableTime() ) );
        S( "host_info", w.GetHostInfo() );
        S( "cpu_manufacturer", w.GetCpuManufacturer() );
        I( "pid", int64_t( w.GetPid() ) );
        I( "timer_resolution_ns", w.GetResolution() );
        I( "sampling_period_ns", w.GetSamplingPeriod() );
        I( "trace_start", w.GetFirstTime() );
        I( "trace_end", w.GetLastTime() );
        I( "trace_dur", w.GetLastTime() - w.GetFirstTime() );
        I( "zone_count", int64_t( w.GetZoneCount() ) );
        I( "gpu_zone_count", int64_t( w.GetGpuZoneCount() ) );
        I( "thread_count", int64_t( t->threads.size() ) );
        I( "source_location_count", int64_t( w.GetSrcLocCount() ) );
        I( "plot_count", int64_t( plots->size() ) );
        I( "message_count", int64_t( w.GetMessages().size() ) );
        I( "lock_count", int64_t( w.GetLockCount() ) );
        I( "context_switch_count", int64_t( w.GetContextSwitchCount() ) );
        I( "callstack_sample_count", int64_t( w.GetCallstackSampleCount() ) );
        auto d = new TableDef();
        d->name = "metadata";
        d->schema = "CREATE TABLE x(id INTEGER, name TEXT, key_type TEXT, int_value INTEGER, str_value TEXT)";
        d->idCol = 0;
        d->rows = [rows] { return int64_t( rows->size() ); };
        d->column = [rows]( sqlite3_context* ctx, int64_t r, int col ) {
            const auto& row = ( *rows )[r];
            switch( col )
            {
            case 0: sqlite3_result_int64( ctx, r ); break;
            case 1: sqlite3_result_text( ctx, row.name.c_str(), -1, SQLITE_TRANSIENT ); break;
            case 2: ResultText( ctx, "single" ); break;
            case 3: if( row.isInt ) sqlite3_result_int64( ctx, row.i ); else sqlite3_result_null( ctx ); break;
            case 4: if( !row.isInt ) sqlite3_result_text( ctx, row.s.c_str(), -1, SQLITE_TRANSIENT ); else sqlite3_result_null( ctx ); break;
            }
        };
        if( !AddTable( t, d, err ) ) return false;
    }

    // Views
    const std::string procName = Quote( w.GetCaptureProgram() );
    const std::string views =
        // Same columns as Perfetto's slices.with_context thread_slice, then the Tracy extras.
        "CREATE VIEW thread_slice AS SELECT id, ts, dur, category, name, track_id, NULL AS track_name, thread_name, utid, tid, "
        "is_main_thread, " + procName + " AS process_name, 0 AS upid, " + std::to_string( pid ) + " AS pid, depth, parent_id, arg_set_id, "
        "thread_ts, thread_dur, source_location_id, file, line, function, zone_name, text, is_fiber FROM slice;"
        "CREATE VIEW thread_or_process_slice AS SELECT * FROM thread_slice;"
        "CREATE VIEW slices AS SELECT * FROM slice;"
        "CREATE VIEW track AS "
        "SELECT id, name, type, parent_id, utid FROM thread_track UNION ALL "
        "SELECT id, name, type, parent_id, NULL FROM counter_track;"
        "CREATE VIEW perfetto_tables AS SELECT name FROM sqlite_schema WHERE type IN ('table', 'view') AND name != 'perfetto_tables' ORDER BY name;";
    return Exec( db, views, err ) == SQLITE_OK;
}

TraceDb* OpenTrace( const char* path, std::string& err )
{
    auto t = new TraceDb();
    t->path = path;
    std::unique_ptr<FileRead> f;
    try
    {
        f.reset( FileRead::Open( path ) );
        if( !f )
        {
            err = std::string( "could not open trace file " ) + path;
            delete t;
            return nullptr;
        }
        // No background tasks: the statistics Tracy would rebuild are not used here.
        t->worker = std::make_unique<Worker>( *f, EventType::All, false );
    }
    catch( const NotTracyDump& )
    {
        err = std::string( "could not load " ) + path + ": not a Tracy trace";
        delete t;
        return nullptr;
    }
    catch( const FileReadError& )
    {
        err = std::string( "could not load " ) + path + ": file read error (truncated or corrupt trace)";
        delete t;
        return nullptr;
    }
    catch( const UnsupportedVersion& e )
    {
        err = std::string( "could not load " ) + path + ": trace was saved by a newer Tracy (file version " +
              std::to_string( e.version >> 16 ) + "." + std::to_string( ( e.version >> 8 ) & 0xFF ) + "." + std::to_string( e.version & 0xFF ) + ")";
        delete t;
        return nullptr;
    }
    catch( const LegacyVersion& e )
    {
        err = std::string( "could not load " ) + path + ": trace file version " +
              std::to_string( e.version >> 16 ) + "." + std::to_string( ( e.version >> 8 ) & 0xFF ) + "." + std::to_string( e.version & 0xFF ) + " is too old; convert it with tracy-update";
        delete t;
        return nullptr;
    }
    catch( const std::exception& e )
    {
        err = std::string( "could not load " ) + path + ": " + e.what();
        delete t;
        return nullptr;
    }
    catch( ... )
    {
        err = std::string( "could not load " ) + path + ": not a Tracy trace, or an unsupported file version";
        delete t;
        return nullptr;
    }
    f.reset();

    const auto& w = t->W();
    int utid = 0;
    for( const auto* td : w.GetThreadData() )
    {
        ThreadInfo ti {};
        ti.tid = td->id;
        ti.name = w.GetThreadName( td->id );
        ti.isFiber = td->isFiber;
        ti.count = td->count;
        const auto n = td->timeline.size();
        if( n > 0 )
        {
            const auto& first = ZoneAt( td->timeline, 0 );
            const auto& last = ZoneAt( td->timeline, n - 1 );
            ti.start = first.Start();
            ti.end = last.IsEndValid() ? last.End() : w.GetLastTime();
            ti.hasTime = true;
        }
        t->threads.push_back( ti );
        t->tidToUtid.emplace( td->id, utid++ );
    }

    if( sqlite3_open( ":memory:", &t->db ) != SQLITE_OK )
    {
        err = "could not create the SQLite database";
        return nullptr;
    }
    if( !RegisterTables( t, err ) ) return nullptr;
    return t;
}

sqlite3* GetSqlite( TraceDb* db )
{
    return db->db;
}

}
