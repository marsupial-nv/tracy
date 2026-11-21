#ifndef __TRACYSQL_HPP__
#define __TRACYSQL_HPP__

#include <stdint.h>
#include <string>

struct sqlite3;

namespace tracysql
{

// A loaded trace plus an in-memory SQLite database whose virtual tables read
// straight from the tracy::Worker.
struct TraceDb;

// Returns nullptr and fills err on failure.
TraceDb* OpenTrace( const char* path, std::string& err );
sqlite3* GetSqlite( TraceDb* db );

struct OutputOptions
{
    enum class Format { Csv, Markdown };
    Format format = Format::Csv;
    bool agent = false;         // JSON errors
    int64_t maxRows = 0;        // 0 = no limit
    int64_t maxBytes = 0;
    int64_t maxCellWidth = 0;
};

struct Sink
{
    virtual ~Sink() {}
    virtual void Out( const char* data, size_t len ) = 0;
    virtual void Err( const char* data, size_t len ) = 0;
};

// Runs every ';'-separated statement of sql, printing each result set.
// Returns the process exit code (0 ok, 1 on the first failing statement).
int RunSql( sqlite3* db, const std::string& sql, const OutputOptions& opts, Sink& sink );

}

#endif
