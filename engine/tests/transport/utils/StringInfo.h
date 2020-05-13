//
// Created by aocsa on 9/25/19.
//

#ifndef BLAZINGDB_COMMUNICATION_TESTS_UTILS_STRINGINFO_H_
#define BLAZINGDB_COMMUNICATION_TESTS_UTILS_STRINGINFO_H_
#include "column_factory.h"
#include <cuda_runtime_api.h>
#include <cudf.h>
#include <driver_types.h>
#include <map>
#include <numeric>

namespace blazingdb {
namespace test {
typedef int nv_category_index_type;

class StringsInfo {
public:
	class StringInfo {
	public:
		explicit StringInfo(gdf_column * column)
			: nvStrings_{nullptr}, stringsLength_{0}, offsetsLength_{0}, stringsPointer_{nullptr},
			  offsetsPointer_{nullptr}, nullBitmask_{nullptr}, stringsSize_{0}, offsetsSize_{0}, nullMaskSize_{0},
			  totalSize_{0} {
			NVCategory * nvCategory = reinterpret_cast<NVCategory *>(column->dtype_info.category);
			if(!nvCategory) {
				nvCategory = NVCategory::create_from_array(nullptr, 0);
			}
			nvStrings_ =
				nvCategory->gather_strings(static_cast<nv_category_index_type *>(column->data), column->size, true);

			if(!nvStrings_) {
				return;
			}

			stringsLength_ = nvStrings_->size();
			offsetsLength_ = stringsLength_ + 1;

			int * const lengthPerStrings = new int[stringsLength_];

			nvStrings_->byte_count(lengthPerStrings, false);
			stringsSize_ = std::accumulate(
				lengthPerStrings, lengthPerStrings + stringsLength_, 0, [](int accumulator, int currentValue) {
					return accumulator + std::max(currentValue, 0);
				});

			offsetsSize_ = offsetsLength_ * sizeof(int);

			stringsPointer_ = new char[stringsSize_];
			offsetsPointer_ = new int[offsetsSize_];

			if(column->null_count > 0) {
				// get_bitmask_size_in_bytes is padded to 64 bytes but only need (stringsLength_ + 7) / 8
				nullMaskSize_ = get_bitmask_size_in_bytes(stringsLength_);
				nullBitmask_ = new unsigned char[nullMaskSize_];
			}
			nvStrings_->create_offsets(stringsPointer_, offsetsPointer_, nullBitmask_, false);
			totalSize_ = stringsSize_ + offsetsSize_ + nullMaskSize_ + 4 * sizeof(const std::size_t);
			delete[] lengthPerStrings;
		}

		~StringInfo() {
			// TODO: remove pointers to map into `result` without bypass
			delete[] stringsPointer_;
			delete[] offsetsPointer_;
			delete[] nullBitmask_;
			NVStrings::destroy(nvStrings_);
		}

		std::size_t stringsLength() const noexcept { return stringsLength_; }

		std::size_t offsetsLength() const noexcept { return offsetsLength_; }

		char * stringsPointer() const noexcept { return stringsPointer_; }

		int * offsetsPointer() const noexcept { return offsetsPointer_; }

		unsigned char * nullBitmask() const noexcept { return nullBitmask_; }

		std::size_t stringsSize() const noexcept { return stringsSize_; }

		std::size_t offsetsSize() const noexcept { return offsetsSize_; }

		std::size_t nullMaskSize() const noexcept { return nullMaskSize_; }

		std::size_t totalSize() const noexcept { return totalSize_; }

	private:
		NVStrings * nvStrings_;
		std::size_t stringsLength_;
		std::size_t offsetsLength_;
		char * stringsPointer_;
		int * offsetsPointer_;
		unsigned char * nullBitmask_;
		std::size_t stringsSize_;
		std::size_t offsetsSize_;
		std::size_t nullMaskSize_;
		std::size_t totalSize_;
	};

	explicit StringsInfo(std::vector<gdf_column *> & columns) {
		for(gdf_column * gdfColumn : columns) {
			if(gdfColumn->dtype == GDF_STRING_CATEGORY) {
				columnMap_.emplace(gdfColumn, new StringInfo{gdfColumn});
			}
		}
	}

	~StringsInfo() {
		std::for_each(columnMap_.cbegin(),
			columnMap_.cend(),
			[](const std::pair<gdf_column * const, StringInfo *> & pair) { delete std::get<StringInfo *>(pair); });
	}

	std::size_t capacity() const noexcept {
		return std::accumulate(columnMap_.cbegin(),
			columnMap_.cend(),
			0,
			[](int & accumulator, const std::pair<gdf_column * const, StringInfo *> & pair) {
				return std::move(accumulator) + std::get<StringInfo *>(pair)->totalSize();
			});
	}

	const StringInfo & At(gdf_column * gdfColumn) const { return *columnMap_.at(gdfColumn); }

private:
	std::map<gdf_column *, StringInfo *> columnMap_;
};

}  // namespace test
}  // namespace blazingdb

#endif  // BLAZINGDB_COMMUNICATION_TESTS_UTILS_STRINGINFO_H_
