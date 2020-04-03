#pragma once

#include <cassert>
#include <cmath>
#include <iomanip>
#include <ios>
#include <iostream>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "any.h"
#include "column.h"
#include "hd.h"
#include "vector.h"

namespace gdf {
namespace library {

class Table {
public:
	Table() = default;  //! \deprecated

	Table & operator=(const Table &) = default;  //! \deprecated

	Table(const std::string & name, std::vector<std::shared_ptr<Column>> && columns)
		: name_{name}, columns_{std::move(columns)} {}

	const Column & operator[](const std::size_t i) const { return *columns_[i]; }

	std::vector<gdf_column_cpp> ToGdfColumnCpps() const;

	bool operator==(const Table & other) const {
		if((name_ != other.name_) || (columns_.size() != other.columns_.size())) {
			std::cerr << "Table: name_ != other.name_  || (columns_.size() != other.columns_.size()"
					  << "\n";
			return false;
		}

		for(std::size_t i = 0; i < columns_.size(); i++) {
			if((*columns_[i]) != (*other.columns_[i])) {
				return false;
			}
		}
		return true;
	}

	std::string name() const { return name_; }

	size_t size() const { return columns_.size(); }

	std::vector<std::string> ColumnNames() const {
		std::vector<std::string> names;
		names.resize(size());
		std::transform(columns_.cbegin(), columns_.cend(), names.begin(), [](const std::shared_ptr<Column> & column) {
			return column->name();
		});
		return names;
	}

	std::vector<std::shared_ptr<Column>>::iterator begin() { return columns_.begin(); }

	std::vector<std::shared_ptr<Column>>::iterator end() { return columns_.end(); }

	template <typename StreamType>
	void print(StreamType & stream) const {
		_size_columns();
		unsigned int cell_padding = 1;
		// Start computing the total width
		// First - we will have size() + 1 "|" characters
		unsigned int total_width = size() + 1;

		// Now add in the size of each colum
		for(auto & col_size : column_sizes_)
			total_width += col_size + (2 * cell_padding);

		// Print out the top line
		stream << "## TABLE_NAME:" << this->name() << "\n";

		stream << std::string(total_width, '-') << "\n";

		std::vector<std::string> headers;
		for(unsigned int i = 0; i < size(); i++)
			headers.push_back(this->columns_[i]->name());

		// Print out the headers
		stream << "|";
		for(unsigned int i = 0; i < size(); i++) {
			// Must find the center of the column
			auto half = column_sizes_[i] / 2;
			half -= headers[i].size() / 2;

			stream << std::string(cell_padding, ' ') << std::setw(column_sizes_[i]) << std::left
				   << std::string(half, ' ') + headers[i] << std::string(cell_padding, ' ') << "|";
		}
		stream << "\n";

		// Print out the line below the header
		stream << std::string(total_width, '-') << "\n";

		// Now print the rows of the VTable
		for(size_t i = 0; i < _num_rows(); i++) {
			stream << "|";

			for(size_t j = 0; j < size(); j++) {
				stream << std::string(cell_padding, ' ') << std::setw(column_sizes_[j]) << columns_[j]->get_as_str(i)
					   << std::string(cell_padding, ' ') << "|";
			}
			stream << "\n";
		}

		// Print out the line below the header
		stream << std::string(total_width, '-') << "\n";
	}

protected:
	void _size_columns() const {
		column_sizes_.resize(size());

		// Temporary for querying each row
		std::vector<unsigned int> column_sizes(size());

		// Start with the size of the headers
		for(unsigned int i = 0; i < size(); i++)
			column_sizes_[i] = this->columns_[i]->name().size();
	}

	size_t _num_rows() const {
		return size() == 0 ? 0 : columns_[0]->size();  //@todo check and assert this
	}

private:
	std::string name_;
	std::vector<std::shared_ptr<Column>> columns_;
	mutable std::vector<unsigned int> column_sizes_;
};

std::vector<gdf_column_cpp> Table::ToGdfColumnCpps() const {
	std::vector<gdf_column_cpp> gdfColumnsCpps;
	gdfColumnsCpps.resize(columns_.size());
	std::transform(
		columns_.cbegin(), columns_.cend(), gdfColumnsCpps.begin(), [](const std::shared_ptr<Column> & column) {
			return column->ToGdfColumnCpp();
		});
	return gdfColumnsCpps;
}

class TableBuilder {
public:
	TableBuilder(const std::string && name, std::vector<ColumnBuilder> builders)
		: name_{std::move(name)}, builders_{builders} {}

	Table Build(const std::size_t length) const {
		std::vector<std::shared_ptr<Column>> columns;
		columns.resize(builders_.size());
		std::transform(
			std::begin(builders_), std::end(builders_), columns.begin(), [length](const ColumnBuilder & builder) {
				return std::move(builder.Build(length));
			});
		return Table(name_, std::move(columns));
	}

	Table Build(const std::vector<std::size_t> & lengths) const {
		std::vector<std::shared_ptr<Column>> columns;
		columns.resize(builders_.size());
		std::transform(
			builders_.cbegin(), builders_.cend(), columns.begin(), [this, lengths](const ColumnBuilder & builder) {
				return std::move(builder.Build(lengths[std::distance(
					builders_.cbegin(), static_cast<std::vector<ColumnBuilder>::const_iterator>(&builder))]));
			});
		return Table(name_, std::move(columns));
	}

private:
	const std::string name_;
	std::vector<ColumnBuilder> builders_;
};

class LiteralTableBuilder : public TableBuilder {
public:
	LiteralTableBuilder(const std::string && name, std::vector<LiteralColumnBuilder> builders)
		: TableBuilder{std::forward<const std::string>(name), ColumnBuildersFrom(builders)}, builders_{builders} {
		lengths_.resize(builders.size());
		std::transform(
			builders.begin(), builders.end(), lengths_.begin(), [](const LiteralColumnBuilder & literalBuilder) {
				return literalBuilder.length();
			});
	}

	Table Build(const std::size_t length = 0) const { return TableBuilder::Build(lengths_); }

private:
	static std::vector<ColumnBuilder> ColumnBuildersFrom(std::vector<LiteralColumnBuilder> & literalBuilders) {
		std::vector<ColumnBuilder> builders;
		builders.resize(literalBuilders.size());
		std::transform(literalBuilders.begin(),
			literalBuilders.end(),
			builders.begin(),
			[](const LiteralColumnBuilder & literalBuilder) { return literalBuilder; });
		return builders;
	}

	std::vector<LiteralColumnBuilder> builders_;
	std::vector<std::size_t> lengths_;
};


class GdfColumnCppsTableBuilder {
public:
	GdfColumnCppsTableBuilder(const std::string & name, const std::vector<gdf_column_cpp> & column_cpps)
		: name_{name}, column_cpps_{column_cpps} {}

	Table Build() { return LiteralTableBuilder{std::move(name_), ColumnBuildersFrom(column_cpps_)}.Build(); }

private:
	std::vector<LiteralColumnBuilder> ColumnBuildersFrom(const std::vector<gdf_column_cpp> & column_cpps) {
		std::vector<LiteralColumnBuilder> builders;
		builders.resize(column_cpps.size());
		std::transform(
			column_cpps.cbegin(), column_cpps.cend(), builders.begin(), [](const gdf_column_cpp & column_cpp) {
				return static_cast<LiteralColumnBuilder>(GdfColumnCppColumnBuilder(column_cpp.name(), column_cpp));
			});
		return builders;
	}

	const std::string name_;
	const std::vector<gdf_column_cpp> column_cpps_;
};

// helper function to tuple_each a tuple of any size
template <class Tuple, typename Func, std::size_t N>
struct TupleEach {
	static void tuple_each(Tuple & t, Func & f) {
		TupleEach<Tuple, Func, N - 1>::tuple_each(t, f);
		f(std::get<N - 1>(t));
	}
};

template <class Tuple, typename Func>
struct TupleEach<Tuple, Func, 1> {
	static void tuple_each(Tuple & t, Func & f) { f(std::get<0>(t)); }
};

template <typename Tuple, typename Func>
void tuple_each(Tuple & t, Func && f) {
	TupleEach<Tuple, Func, std::tuple_size<Tuple>::value>::tuple_each(t, f);
}
// end helper function

struct _GetValuesLambda {
	size_t & i;
	size_t & j;
	std::vector<std::vector<linb::any>> & values;

	template <typename T>
	void operator()(T && value) const {
		values[j][i] = value;
		j++;
	}
};

struct _FillColumnLambda {
	std::vector<std::vector<linb::any>> & values;
	std::vector<ColumnFiller> & builders;
	std::vector<std::string> headers;
	mutable size_t i;

	_FillColumnLambda(std::vector<std::vector<linb::any>> & values,
		std::vector<ColumnFiller> & builders,
		std::vector<std::string> & headers)
		: values{values}, builders{builders}, headers{headers}, i{0} {}

	template <typename T>
	void operator()(T value) const {
		assert(i < headers.size());
		auto name = headers[i];
		std::vector<decltype(value)> column_values;
		for(auto && any_val : values[i]) {
			column_values.push_back(linb::any_cast<decltype(value)>(any_val));
		}
		builders.push_back(ColumnFiller{name, column_values});
		i++;
	}
};

template <class... Ts>
class TableRowBuilder {
public:
	typedef std::tuple<Ts...> DataTuple;

	TableRowBuilder(const std::string & name, std::vector<std::string> headers, std::initializer_list<DataTuple> rows)
		: name_{name}, headers_(headers), rows_{rows}, ncols_{std::tuple_size<DataTuple>::value}, nrows_{rows.size()} {}

	Table Build() {
		size_t i = 0;
		std::vector<std::vector<linb::any>> values(ncols_, std::vector<linb::any>(nrows_));
		for(DataTuple row : rows_) {
			size_t j = 0;
			tuple_each(row, _GetValuesLambda{i, j, values});
			i++;
		}
		std::vector<ColumnFiller> builders;
		tuple_each(rows_[0], _FillColumnLambda{values, builders, this->headers_});

		std::vector<std::shared_ptr<Column>> columns;
		columns.resize(builders.size());
		std::transform(std::begin(builders), std::end(builders), columns.begin(), [](ColumnFiller & builder) {
			return std::move(builder.Build());
		});
		return Table(name_, std::move(columns));
	}

private:
	const std::string name_;
	const size_t nrows_;
	const size_t ncols_;
	std::vector<std::string> headers_;
	std::vector<DataTuple> rows_;
};

}  // namespace library
}  // namespace gdf
