#define DUCKDB_EXTENSION_MAIN

#include "roaring_msisdn_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "common.h"

namespace duckdb {

// Forward declarations
void MsisdnNormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result);
void BitmapHasFunction(DataChunk &args, ExpressionState &state, Vector &result);
void BitmapCardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result);
void BitmapIntersectionFunction(DataChunk &args, ExpressionState &state, Vector &result);
void BitmapUnionFunction(DataChunk &args, ExpressionState &state, Vector &result);
void BitmapDifferenceFunction(DataChunk &args, ExpressionState &state, Vector &result);

// Aggregate function declarations
struct BitmapAggState {
	roaring64_bitmap_t *bitmap;
	
	BitmapAggState() : bitmap(roaring64_bitmap_create()) {}
	
	~BitmapAggState() {
		if (bitmap) {
			roaring64_bitmap_free(bitmap);
		}
	}
	
	BitmapAggState(const BitmapAggState &other) {
		bitmap = roaring64_bitmap_copy(other.bitmap);
	}
	
	BitmapAggState &operator=(const BitmapAggState &other) {
		if (this != &other) {
			if (bitmap) {
				roaring64_bitmap_free(bitmap);
			}
			bitmap = roaring64_bitmap_copy(other.bitmap);
		}
		return *this;
	}
};

struct BitmapAggOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.bitmap = roaring64_bitmap_create();
	}
	
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (input > 0) {
			roaring64_bitmap_add(state.bitmap, (uint64_t)input);
		}
	}
	
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count) {
		if (input > 0) {
			for (idx_t i = 0; i < count; i++) {
				roaring64_bitmap_add(state.bitmap, (uint64_t)input);
			}
		}
	}
	
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.bitmap && target.bitmap) {
			roaring64_bitmap_or_inplace(target.bitmap, source.bitmap);
		} else if (source.bitmap && !target.bitmap) {
			target.bitmap = roaring64_bitmap_copy(source.bitmap);
		} else if (!source.bitmap && target.bitmap) {
			// Source is empty, target has data - keep target
			// No action needed
		} else {
			// Both are empty - no action needed
		}
	}
	
	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (!state.bitmap) {
			target = string_t("");
			return;
		}
		
		auto serialized = serialize_roaring64(state.bitmap);
		if (serialized.empty()) {
			target = string_t("");
		} else {
			target = string_t((char*)serialized.data(), serialized.size());
		}
	}
	
	template <class STATE, class OP>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.bitmap) {
			roaring64_bitmap_free(state.bitmap);
			state.bitmap = nullptr;
		}
	}
	
	static bool IgnoreNull() {
		return true;
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	// Register msisdn_normalize(text) -> BIGINT
	auto msisdn_normalize_function = ScalarFunction("msisdn_normalize", {LogicalType::VARCHAR}, LogicalType::BIGINT, MsisdnNormalizeFunction);
	loader.RegisterFunction(msisdn_normalize_function);
	
	// Register bitmap_has(blob, bigint) -> BOOLEAN
	auto bitmap_has_function = ScalarFunction("bitmap_has", {LogicalType::BLOB, LogicalType::BIGINT}, LogicalType::BOOLEAN, BitmapHasFunction);
	loader.RegisterFunction(bitmap_has_function);
	
	// Register bitmap_cardinality(blob) -> BIGINT
	auto bitmap_cardinality_function = ScalarFunction("bitmap_cardinality", {LogicalType::BLOB}, LogicalType::BIGINT, BitmapCardinalityFunction);
	loader.RegisterFunction(bitmap_cardinality_function);
	
	// Register bitmap_intersection(blob, blob) -> BLOB
	auto bitmap_intersection_function = ScalarFunction("bitmap_intersection", {LogicalType::BLOB, LogicalType::BLOB}, LogicalType::BLOB, BitmapIntersectionFunction);
	loader.RegisterFunction(bitmap_intersection_function);
	
	// Register bitmap_union(blob, blob) -> BLOB
	auto bitmap_union_function = ScalarFunction("bitmap_union", {LogicalType::BLOB, LogicalType::BLOB}, LogicalType::BLOB, BitmapUnionFunction);
	loader.RegisterFunction(bitmap_union_function);
	
	// Register bitmap_difference(blob, blob) -> BLOB
	auto bitmap_difference_function = ScalarFunction("bitmap_difference", {LogicalType::BLOB, LogicalType::BLOB}, LogicalType::BLOB, BitmapDifferenceFunction);
	loader.RegisterFunction(bitmap_difference_function);
	
	// Register bitmap_agg aggregate function
	AggregateFunction bitmap_agg_function = AggregateFunction::UnaryAggregate<BitmapAggState, int64_t, string_t, BitmapAggOperation>(
		LogicalType::BIGINT, LogicalType::BLOB);
	bitmap_agg_function.name = "bitmap_agg";
	loader.RegisterFunction(bitmap_agg_function);
}

void RoaringMsisdnExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RoaringMsisdnExtension::Name() {
	return "roaring_msisdn";
}

std::string RoaringMsisdnExtension::Version() const {
#ifdef EXT_VERSION_ROARING_MSISDN
	return EXT_VERSION_ROARING_MSISDN;
#else
	return "";
#endif
}


} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(roaring_msisdn, loader) {
	duckdb::LoadInternal(loader);
}
}
