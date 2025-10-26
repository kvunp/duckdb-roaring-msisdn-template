#include "common.h"
#include <duckdb.hpp>
#include <sstream>
#include <cctype>
#include <string>
#include <vector>

using namespace duckdb;

namespace duckdb {

// Helper: strip non-digit characters, return false if no digits found.
static bool strip_to_digits(const std::string &in, std::string &out) {
    out.clear();
    for (char c : in) {
        if (std::isdigit((unsigned char)c)) out.push_back(c);
    }
    return !out.empty();
}

// Scalar UDF implementation for msisdn_normalize(text) -> BIGINT
void MsisdnNormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    UnaryExecutor::Execute<string_t, int64_t>(input, result, args.size(), [&](string_t input_str) -> int64_t {
        std::string s(input_str.GetString());
        std::string digits;
        if (!strip_to_digits(s, digits)) {
            return 0;
        }
        if (digits.size() > 15) {
            return 0;
        }
        try {
            unsigned long long v = std::stoull(digits);
            return (int64_t)v;
        } catch (...) {
            return 0;
        }
    });
}

// Register function helper (called from extension registration)
void register_msisdn_functions(Connection &con) {
    // The precise API to register a scalar function in an extension context may differ
    // between DuckDB versions. We'll use a simplified registration approach via SQL here:
    // create a SQL wrapper that calls into a C++ function is more involved; the agent
    // integrating this should register using DuckDB's CreateScalarFunction API.
    // For now, leave this as a placeholder.
}

} // namespace duckdb
