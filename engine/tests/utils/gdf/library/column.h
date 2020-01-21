#pragma once

#include <cassert>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "any.h"
#include "definitions.h"
#include "hd.h"
#include "types.h"
#include "vector.h"

#include "from_cudf/cpp_src/utilities/legacy/bit_util.cuh"
#include <arrow/util/bit_util.h>


namespace gdf {
namespace library {

class Column {
public:
	Column(const std::string & name) : name_{name} {}

	virtual ~Column();

	virtual gdf_column_cpp ToGdfColumnCpp() const = 0;

	virtual const void * get(const std::size_t i) const = 0;

	bool operator==(const Column & other) const {
		for(std::size_t i = 0; i < size(); i++) {
			if((*static_cast<const std::uint64_t *>(get(i))) != (*static_cast<const std::uint64_t *>(other.get(i)))) {
				return false;
			}
		}
		return true;
	}
	bool is_valid(size_t i) const {
		return valids_.size() == 0 ? true : valids_[i >> size_t(3)] & (1 << (i & size_t(7)));
	}

	bool operator!=(const Column & other) const { return !(*this == other); }

	class Wrapper {
	public:
		template <class T>
		operator T() const {
			return *static_cast<const T *>(column->get(i));
		}

		template <gdf_dtype DType>
		typename DTypeTraits<DType>::value_type get() const {
			return static_cast<typename DTypeTraits<DType>::value_type>(*this);
		}

		const std::size_t i;
		const Column * column;
	};

	Wrapper operator[](const std::size_t i) const { return Wrapper{i, this}; }

	virtual size_t size() const = 0;

	virtual std::string to_string() const = 0;

	virtual std::string get_as_str(int index) const = 0;

	std::string name() const { return name_; }

public:
	const std::vector<cudf::valid_type> & getValids() const { return valids_; }

	void setValids(const std::vector<cudf::valid_type> && valids) { valids_ = std::move(valids); }

protected:
	static gdf_column_cpp Create(const std::string & name,
		const gdf_dtype dtype,
		const std::size_t length,
		const void * data,
		const cudf::valid_type * valid,
		const std::size_t size);

protected:
	const std::string name_;
	std::vector<cudf::valid_type> valids_;
};

Column::~Column() {}


// TODO: this is slow as shit
void convert_bools_to_valids(cudf::valid_type * valid_ptr, const std::vector<short> & input) {
	for(std::size_t row_index = 0; row_index < input.size(); row_index++) {
		if(input[row_index]) {
			gdf::util::turn_bit_on(valid_ptr, row_index);
		} else {
			gdf::util::turn_bit_off(valid_ptr, row_index);
		}
	}
}

gdf_column_cpp Column::Create(const std::string & name,
	const gdf_dtype dtype,
	const std::size_t length,
	const void * data,
	const cudf::valid_type * valid,
	const std::size_t size) {
	gdf_column_cpp column_cpp;
	gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
	column_cpp.create_gdf_column(
		dtype, extra_info, length, const_cast<void *>(data), const_cast<cudf::valid_type *>(valid), size, "");
	column_cpp.set_name(name);
	return column_cpp;
}

template <gdf_dtype value>
using Ret = DType<value>;  //! \deprecated

template <gdf_dtype DType>
class TypedColumn : public Column {
public:
	using value_type = typename DTypeTraits<DType>::value_type;
	using Callback = std::function<value_type(const std::size_t)>;

	TypedColumn(const std::string & name) : Column(name) {}

	void range(const std::size_t begin, const std::size_t end, Callback callback) {
		// assert(end > begin);  // TODO(gcca): bug with gdf_column_cpps
		values_.reserve(end - begin);
		for(std::size_t i = begin; i < end; i++) {
			values_.push_back(callback(i));
		}
	}

	void range(const std::size_t end, Callback callback) { range(0, end, callback); }

	gdf_column_cpp ToGdfColumnCpp() const final {
		return Create(name(), DType, this->size(), values_.data(), valids_.data(), sizeof(value_type));
	}

	value_type operator[](const std::size_t i) const { return values_[i]; }

	const void * get(const std::size_t i) const final { return &values_[i]; }

	void FillData(std::vector<value_type> values) {
		for(std::size_t i = 0; i < values.size(); i++) {
			values_.push_back(values.at(i));
		}
	}

	size_t size() const final { return values_.size(); }

	std::string to_string() const final {
		std::ostringstream stream;

		for(std::size_t i = 0; i < values_.size(); i++) {
			if(this->is_valid(i)) {
				stream << values_.at(i) << ",";

			} else {
				stream << "@"
					   << ",";
			}
		}
		return std::string{stream.str()};
	}

	std::string get_as_str(int index) const final {
		//@todo, use valids_ !
		std::ostringstream out;
		if(std::is_floating_point<value_type>::value) {
			out.precision(1);
		}
		if(sizeof(value_type) == 1) {
			if(this->is_valid(index))
				out << std::fixed << (int) values_.at(index);
			else
				out << std::fixed << "@";

		} else {
			if(this->is_valid(index))
				out << std::fixed << values_.at(index);
			else
				out << std::fixed << "@";
		}
		return out.str();
	}

private:
	std::basic_string<value_type> values_;
};

template <class T>
class RangeTraits : public RangeTraits<decltype(&std::remove_reference<T>::type::operator())> {};

template <class C, class R, class... A>
class RangeTraits<R (C::*)(A...) const> {
public:
	typedef R r_type;
};

class ColumnBuilder {
public:
	template <class Callback>
	ColumnBuilder(const std::string & name, Callback && callback)
		: impl_{std::make_shared<Impl<Callback>>(std::move(name), std::forward<Callback>(callback))} {}

	template <class Callback>
	ColumnBuilder(const std::string & name, std::vector<cudf::valid_type> && valids, Callback && callback)
		: impl_{
			  std::make_shared<Impl<Callback>>(std::move(name), std::move(valids), std::forward<Callback>(callback))} {}

	ColumnBuilder() {}
	ColumnBuilder & operator=(const ColumnBuilder & other) {
		impl_ = other.impl_;
		return *this;
	}

	std::unique_ptr<Column> Build(const std::size_t length) const { return impl_->Build(length); }

private:
	class ImplBase {
	public:
		inline virtual ~ImplBase();
		virtual std::unique_ptr<Column> Build(const std::size_t length) = 0;
	};

	template <class Callable>
	class Impl : public ImplBase {
	public:
		Impl(const std::string && name, Callable && callback)
			: name_{std::move(name)}, callback_{std::forward<Callable>(callback)} {}

		Impl(const std::string && name, const std::vector<cudf::valid_type> && valids, Callable && callback)
			: name_{std::move(name)}, valids_{std::move(valids)}, callback_{std::forward<Callable>(callback)} {}

		std::unique_ptr<Column> Build(const std::size_t length) final {
			auto * column = new TypedColumn<RangeTraits<decltype(callback_)>::r_type::value>(name_);
			column->range(length, callback_);
			column->setValids(std::move(valids_));
			return std::unique_ptr<Column>(column);
		}

	private:
		using valid_vector = std::vector<cudf::valid_type>;

	private:
		const std::string name_;
		const valid_vector valids_;
		Callable callback_;
	};

	std::shared_ptr<ImplBase> impl_;
};

inline ColumnBuilder::ImplBase::~ImplBase() = default;


template <gdf_dtype id>
class Literals {
public:
	using value_type = typename DType<id>::value_type;
	using valid_type = cudf::valid_type;
	using initializer_list = std::initializer_list<value_type>;
	using vector = std::vector<value_type>;
	using valid_vector = std::vector<valid_type>;
	using bool_vector = std::vector<short>;


	Literals(const vector && values) : values_{std::move(values)} {}
	Literals(const initializer_list & values) : values_{values} {}

	Literals(const vector && values, valid_vector && valids) : values_{std::move(values)}, valids_{std::move(valids)} {}


	Literals(const vector & values, const bool_vector & bools)
		: values_{values}, valids_(gdf_valid_allocation_size(values.size()), 0) {
		assert(values.size() == bools.size());
		cudf::valid_type * host_valids = (cudf::valid_type *) valids_.data();
		convert_bools_to_valids(host_valids, bools);
		// std::cout << "\tafter: " <<  gdf::util::gdf_valid_to_str(host_valids, values.size()) << std::endl;
	}

	const vector & values() const { return values_; }

	valid_vector valids() { return valids_; }

	std::size_t size() const { return values_.size(); }

	value_type operator[](const std::size_t i) const { return values_[i]; }

private:
	vector values_;
	valid_vector valids_;
};

class LiteralColumnBuilder {
public:
	enum class Path { Output };

public:
	template <gdf_dtype id>
	LiteralColumnBuilder(const std::string & name, Literals<id> values)
		: impl_{std::make_shared<Impl<id>>(name, values)} {}

	template <gdf_dtype id>
	LiteralColumnBuilder(Path path, const std::string & name, Literals<id> values)
		: impl_{std::make_shared<OutImpl<id>>(name, values)} {}

	LiteralColumnBuilder() {}
	LiteralColumnBuilder & operator=(const LiteralColumnBuilder & other) {
		impl_ = other.impl_;
		return *this;
	}

	std::unique_ptr<Column> Build() const { return impl_->Build(); }

	operator ColumnBuilder() const { return *impl_; }

	std::size_t length() const { return impl_->length(); }

private:
	class ImplBase {
	public:
		inline virtual ~ImplBase();
		virtual std::unique_ptr<Column> Build() = 0;
		virtual operator ColumnBuilder() const = 0;
		virtual std::size_t length() const = 0;
	};

	template <gdf_dtype id>
	class Impl : public ImplBase {
	public:
		Impl(const std::string & name, Literals<id> literals)
			: name_{name}, literals_{literals}, builder_{ColumnBuilder(
													name_, std::move(literals.valids()), [this](Index i) -> DType<id> {
														return *(literals_.values().begin() +
																 static_cast<std::ptrdiff_t>(i));
													})} {}

		std::unique_ptr<Column> Build() { return builder_.Build(literals_.size()); }

		operator ColumnBuilder() const { return builder_; }

		virtual std::size_t length() const { return literals_.size(); }

	private:
		const std::string name_;
		Literals<id> literals_;
		ColumnBuilder builder_;
	};

	template <gdf_dtype id>
	class OutImpl : public ImplBase {
	public:
		OutImpl(const std::string & name, Literals<id> & literals)
			: name_{name}, literals_{literals}, builder_{ColumnBuilder(
													name_, std::move(literals_.valids()), [this](Index i) -> DType<id> {
														return *(literals_.values().begin() +
																 static_cast<std::ptrdiff_t>(i));
													})} {}

		std::unique_ptr<Column> Build() { return builder_.Build(literals_.size()); }

		operator ColumnBuilder() const { return builder_; }

		virtual std::size_t length() const { return literals_.size(); }

	private:
		const std::string name_;
		Literals<id> literals_;
		ColumnBuilder builder_;
	};

	std::shared_ptr<ImplBase> impl_;
};

inline LiteralColumnBuilder::ImplBase::~ImplBase() = default;

class GdfColumnCppColumnBuilder {
public:
	GdfColumnCppColumnBuilder(const std::string & name, const gdf_column_cpp & column_cpp)
		: name_{name}, column_cpp_{column_cpp} {}

	GdfColumnCppColumnBuilder() = default;
	GdfColumnCppColumnBuilder & operator=(const GdfColumnCppColumnBuilder & other) {
		name_ = other.name_;
		column_cpp_ = other.column_cpp_;
		return *this;
	}

	operator LiteralColumnBuilder() const {
		auto column_cpp = const_cast<gdf_column_cpp &>(column_cpp_);
		switch(column_cpp.dtype()) {
#define CASE(D)                                                                                                        \
	case GDF_##D:                                                                                                      \
		return LiteralColumnBuilder {                                                                                  \
			LiteralColumnBuilder::Path::Output, name_, Literals<GDF_##D> {                                             \
				std::move(HostVectorFrom<GDF_##D>(column_cpp)), std::move(HostValidFrom(column_cpp))                   \
			}                                                                                                          \
		}
			CASE(INT8);
			CASE(INT16);
			CASE(INT32);
			CASE(INT64);

			// TODO percy noboa see upgrade to uints
			//      CASE(UINT8);
			//      CASE(UINT16);
			//      CASE(UINT32);
			//      CASE(UINT64);

			CASE(FLOAT32);
			CASE(FLOAT64);
			CASE(BOOL8);
			CASE(DATE32);
			CASE(DATE64);
			CASE(TIMESTAMP);
#undef CASE
		default: throw std::runtime_error("Bad DType: " + std::to_string(column_cpp.dtype()));
		}
	}

	std::unique_ptr<Column> Build() const { return static_cast<LiteralColumnBuilder>(*this).Build(); }

private:
	std::string name_;
	gdf_column_cpp column_cpp_;
};

class ColumnFiller {
public:
	template <class Type>
	ColumnFiller(const std::string & name, const std::vector<Type> & values) {
		auto * pointer = new TypedColumn<GdfDataType<Type>::Value>(name);
		pointer->FillData(values);
		column_ = std::shared_ptr<Column>(pointer);
	}
	ColumnFiller() = default;
	ColumnFiller(const ColumnFiller & other) = default;
	ColumnFiller & operator=(const ColumnFiller & other) = default;

	std::shared_ptr<Column> Build() const { return column_; }

private:
	std::shared_ptr<Column> column_;
};

}  // namespace library
}  // namespace gdf
