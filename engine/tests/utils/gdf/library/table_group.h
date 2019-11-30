#pragma once
#include "column.h"
#include "table.h"
#include <blazingdb/io/Util/StringUtil.h>

namespace gdf {
namespace library {

using BlazingFrame = std::vector<std::vector<gdf_column_cpp>>;

class TableGroup {
public:
	TableGroup(std::initializer_list<Table> tables) {
		for(Table table : tables) {
			tables_.push_back(table);
		}
	}

	TableGroup(const std::vector<Table> & tables) : tables_{tables} {}

	BlazingFrame ToBlazingFrame() const;

	std::vector<std::string> table_names() const {
		std::vector<std::string> names;
		for(auto & table : tables_) {
			std::string name = table.name();
			// TODO: Workaorund no normalize table names. Delete this later
			if(StringUtil::beginsWith(name, "main.")) {
				name = name.substr(5);
			}
			names.push_back(name);
		}
		return names;
	}

	std::vector<std::vector<std::string>> column_names() {
		std::vector<std::vector<std::string>> list_of_names;
		for(auto & table : tables_) {
			std::vector<std::string> names;
			for(std::shared_ptr<Column> & column : table) {
				names.push_back(column->name());
			}
			list_of_names.push_back(names);
		}
		return list_of_names;
	}
	size_t size() const { return tables_.size(); }

	const Table & operator[](const std::size_t i) const { return tables_[i]; }

private:
	std::vector<Table> tables_;
};

BlazingFrame TableGroup::ToBlazingFrame() const {
	BlazingFrame frame;
	frame.resize(tables_.size());
	std::transform(
		tables_.cbegin(), tables_.cend(), frame.begin(), [](const Table & table) { return table.ToGdfColumnCpps(); });
	return frame;
}

template <class TableBuilderType>
class TypedTableGroupBuilder {
public:
	TypedTableGroupBuilder(std::initializer_list<TableBuilderType> builders) : builders_(builders) {}

	TableGroup Build(const std::initializer_list<const std::size_t> lengths) {
		std::vector<Table> tables;
		tables.resize(builders_.size());
		std::transform(std::begin(builders_),
			std::end(builders_),
			tables.begin(),
			[this, lengths](const TableBuilderType & builder) {
				return builder.Build(*(std::begin(lengths) + std::distance(std::begin(builders_), &builder)));
			});
		return TableGroup(tables);
	}

	TableGroup Build(const std::size_t length = 0) {
		std::vector<Table> tables;
		tables.resize(builders_.size());
		std::transform(
			std::begin(builders_), std::end(builders_), tables.begin(), [length](const TableBuilderType & builder) {
				return builder.Build(length);
			});
		return TableGroup{tables};
	}

private:
	std::initializer_list<TableBuilderType> builders_;
};

using Index = const std::size_t;

using TableGroupBuilder = TypedTableGroupBuilder<TableBuilder>;
using LiteralTableGroupBuilder = TypedTableGroupBuilder<LiteralTableBuilder>;

}  // namespace library
}  // namespace gdf
