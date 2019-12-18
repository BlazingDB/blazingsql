SqlCall SqlUseDatabase() :
{
    final SqlParserPos pos;
    final SqlIdentifier id;
}
{
	<USE> { pos = getPos(); } 
    <DATABASE> id = CompoundIdentifier()
    {
        return SqlSimplicityNodes.useDatabase(pos, id);
    }
}

SqlCall SqlListDatabases() :
{
    final SqlParserPos pos;
}
{
	<LIST> { pos = getPos(); } 
    <DATABASES>
    {
        return SqlSimplicityNodes.listDatabases(pos);
    }
}

SqlCall SqlListTables() :
{
    final SqlParserPos pos;
}
{
	<LIST> { pos = getPos(); } 
    <TABLES>
    {
        return SqlSimplicityNodes.listTables(pos);
    }
}

SqlCall SqlListViews() :
{
    final SqlParserPos pos;
}
{
	<LIST> { pos = getPos(); } 
    <VIEWS>
    {
        return SqlSimplicityNodes.listTables(pos);
    }
}

SqlCall SqlGetDataFolders() :
{
    final SqlParserPos pos;
}
{
	<GET> { pos = getPos(); } 
    <DATA> <FOLDERS>
    {
        return SqlSimplicityNodes.getDataFolders(pos);
    }
}

SqlCall SqlGetUploadFolders() :
{
    final SqlParserPos pos;
}
{
	<GET> { pos = getPos(); } 
    <UPLOAD> <FOLDERS>
    {
        return SqlSimplicityNodes.getUploadFolders(pos);
    }
}

SqlCall SqlGetPerfFolders() :
{
    final SqlParserPos pos;
}
{
	<GET> { pos = getPos(); } 
    <PERF> <FOLDERS>
    {
        return SqlSimplicityNodes.getPerfFolders(pos);
    }
}

SqlCall SqlExitBlazing() :
{
    final SqlParserPos pos;
}
{
	<EXIT_BLAZING> { pos = getPos(); } 
    {
        return SqlSimplicityNodes.exitBlazing(pos);
    }
}

// End parserImplsSimplicity.ftl
