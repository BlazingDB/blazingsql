set(provider_sources
    provider_test.cpp
)

configure_test(provider_test "${provider_sources}")

# TODO percy c.gonzales add separated tests and add parser tests too
if(MYSQL_SUPPORT AND SQLITE_SUPPORT)
    set(sql_provider_sources
        sql_provider_test.cpp
    )
    
    configure_test(sql_provider_test "${sql_provider_sources}")
endif()