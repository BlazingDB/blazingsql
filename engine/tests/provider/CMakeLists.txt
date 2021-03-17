set(provider_sources
    provider_test.cpp
)

configure_test(provider_test "${provider_sources}")

set(sql_provider_sources
    sql_provider_test.cpp
)

configure_test(sql_provider_test "${sql_provider_sources}")
