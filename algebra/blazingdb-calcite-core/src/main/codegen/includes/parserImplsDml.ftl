SqlCall SqlLoadDataInfile() :
{
    final SqlParserPos pos;
    final SqlNode file;
    final SqlIdentifier table;
    final SqlNode delimiter;
    final SqlNode quoteCharacter;
    final SqlNode termination;
}
{
    <LOAD> <DATA> <INFILE> { pos = getPos(); } 
    file = StringLiteral()
    <INTO> <TABLE>
    table = CompoundIdentifier()
    <FIELDS> <TERMINATED> <BY>
    delimiter = StringLiteral()
    <ENCLOSED> <BY>
    quoteCharacter = StringLiteral()
    <LINES> <TERMINATED> <BY>
    termination = StringLiteral()
    {
        return SqlDmlNodes.loadDataInfile(pos, file, table, delimiter, quoteCharacter, termination);
    }
}

// End parserImplsDml.ftl
