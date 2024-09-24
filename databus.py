
import requests

import main

DATABUS_API = "https://databus.openenergyplatform.org"
DATABUS_ENDPOINT = "https://databus.openenergyplatform.org/sparql"

def query_sparql(query: str) -> dict:
    """
    Query SPARQL endpoint and return data as dict.

    Parameters
    ----------
    query: str
        SPARQL query to be executed

    Returns
    -------
    dict
        SPARQL results as dict
    """
    response = requests.post(
        DATABUS_ENDPOINT,
        headers={"Accept": "application/json, text/plain, */*", "Content-Type": "application/x-www-form-urlencoded"},
        data={"query": query},
        timeout=90,
    )
    data = response.json()
    return data["results"]["bindings"]


def delete_artifact(artifact_name: str) -> None:
    """Delete artifact on databus."""
    headers = {"X-API-KEY": f"{main.DATABUS_API_KEY}"}
    url = f"{DATABUS_API}/{main.DATABUS_USER}/{main.DATABUS_GROUP}/{artifact_name}"
    response = requests.delete(url, headers=headers)
    if response.status_code != 204:
        main.logger.error(f"Failed to delete artifact {artifact_name}.")
    else:
        main.logger.info(f"Deleted artifact {artifact_name}.")


def delete_all_artifact_versions(artifact_name: str) -> None:
    """Delete all artifact versions on databus for given artifact name."""
    url = f"{DATABUS_API}/{main.DATABUS_USER}/{main.DATABUS_GROUP}/{artifact_name}"
    versions = get_artifact_versions(url)
    headers = {"X-API-KEY": f"{main.DATABUS_API_KEY}"}

    for version in versions:
        version_url = f"{url}/{version}"
        response = requests.delete(version_url, headers=headers)
        if response.status_code != 204:
            main.logger.error(f"Failed to delete version '{version}' of artifact {artifact_name}.")
        else:
            main.logger.info(f"Deleted version '{version}' of artifact {artifact_name}.")


def get_artifact_versions(artifact_url: str) -> list[str]:
    """Return all versions belonging to the given artifact URL."""
    query = f"""
            PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX dcat:   <http://www.w3.org/ns/dcat#>
            PREFIX dct:    <http://purl.org/dc/terms/>
            PREFIX dcv:    <https://dataid.dbpedia.org/databus-cv#>
            PREFIX dataid: <https://dataid.dbpedia.org/databus#>
            SELECT ?version WHERE
            {{
                GRAPH ?g
                {{
                    ?dataset dataid:artifact <{artifact_url}> .
                    ?dataset dct:hasVersion ?version .
                }}
            }} ORDER BY DESC (?version)
            """
    result = query_sparql(query)
    versions = [version["version"]["value"] for version in result]
    return versions


if __name__ == "__main__":
    main.load_databus_credentials()
    _artifact_name = input(f"Artifact to delete on databus at '{main.DATABUS_USER}/{main.DATABUS_GROUP}': ")
    delete_all_artifact_versions(artifact_name=_artifact_name)
    delete_artifact(artifact_name=_artifact_name)