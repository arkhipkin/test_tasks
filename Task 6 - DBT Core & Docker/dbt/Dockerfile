# =============================================================================
# NEW (MVP scaffold) — NOT used for Okta SSO.
# externalbrowser auth needs a host browser — run dbt locally instead.
# Revisit after key-pair (or service-user) auth is available.
# =============================================================================

FROM ghcr.io/dbt-labs/dbt-snowflake:1.9.0

COPY profiles.yml /root/.dbt/profiles.yml
COPY . /usr/app/dbt

WORKDIR /usr/app/dbt

ENTRYPOINT ["dbt"]
CMD ["debug"]

# Future (key-pair) example:
# docker build -t anton-dbt-mvp .
# docker run --rm \
#   -e SF_ACCOUNT -e DBT_TECH_USER -e SF_ROLE -e SF_WAREHOUSE -e DB_DEV -e DBT_SCHEMA \
#   -v "$SNOWFLAKE_PRIVATE_KEY_PATH:/keys/rsa_key.p8:ro" \
#   -e SNOWFLAKE_PRIVATE_KEY_PATH=/keys/rsa_key.p8 \
#   anton-dbt-mvp seed --select sample_countries







# =============================================================================
# OLD working version — kept commented for history / reuse
# =============================================================================

# FROM ghcr.io/dbt-labs/dbt-snowflake:1.3.0
#
# ## Copy profiles.yml
# COPY profiles.yml /root/.dbt/
#
# ## Copy dbt projects
# #CMD mkdir -p dbt/bi_dbt_crm
# RUN mkdir -p dbt/bi_dbt_crm
# COPY . dbt/bi_dbt_crm/
# # Macros which is used for the local development and separation of the developers' schemas is in the .dockerignore
# # And the macro for the same target schema needs to be renamed to be visible for DBT
# COPY macros/docker/get_custom_schema dbt/bi_dbt_crm/macros/docker/get_custom_schema.sql
#
# # Set environment variablesecho
# ENV DBT_DIR dbt/bi_dbt_crm/
#
# # specify work directory as root
# WORKDIR $DBT_DIR
#
# RUN pwd
# # /usr/app/dbt/dbt/bi_dbt_crm
#
# ENTRYPOINT [""]
# # #CMD ["sh"]
#
#
#
#
#
# # To test:
# # docker build . -t docker_dbt_crm
# # export DBT_ENV=INT/PROD
# # export DBT_PASSWORD=xxx
# # docker run -e DBT_PASSWORD -e DBT_ENV -ti docker_dbt_crm sh
# # cd bi_dbt_crm
# # dbt run
# # Ctrl + D to deattach from the running image
#
# # docker run -e DBT_PASSWORD -e DBT_ENV docker_dbt_crm "dbt" "run" "--project-dir" "bi_dbt_crm" "--select" "personal"
# # docker run -e DBT_PASSWORD -e DBT_ENV docker_dbt_crm "dbt" "run" "--select" "personal"
# # docker run -e DBT_PASSWORD -e DBT_ENV docker_dbt_crm "sh" "dbt run --select personal"
# # docker run -e DBT_PASSWORD -e DBT_ENV docker_dbt_crm "sh" "dbt_executions/test_execution.sh"
