set(provider_sources
    provider_test.cpp
)

configure_test(provider_test "${provider_sources}")

set(sql_transpiler_sources
    sql_transpiler_test.cpp
)

configure_test(sql_transpiler_test "${sql_transpiler_sources}")

# TODO percy c.gonzales add separated tests and add parser tests too
if(MYSQL_SUPPORT AND SQLITE_SUPPORT)
    set(sql_provider_sources
        sql_provider_test.cpp
    )
    configure_test(sql_provider_test "${sql_provider_sources}")
endif()