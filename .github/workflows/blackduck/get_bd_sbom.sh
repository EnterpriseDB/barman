#!/usr/bin/env bash

set -E

#### Functions

function show_banner {
  echo "=============================================================="
  echo "                  Create Blackduck SBOM                       "
  echo "=============================================================="
  echo
}

function show_usage {
    echo
    echo "Usage: ./get-bd-sbom.sh <blackduck-url> <blackduck-api-token> <project-name> <version-name>"
}

function get_bearer {
  result=$(curl --silent --location --request POST "${blackduck_url}api/tokens/authenticate" \
    --header "Authorization: token $blackduck_token")
  if [[ $(echo "$result" | jq -r .errorCode) != null ]]
  then
    >&2 echo "ERROR: No bearer token found"
    exit 1
  else
    echo "$result" | jq -r .bearerToken
  fi
}

function get_project_id {
  result=$(curl --silent --location --request GET "${blackduck_url}api/projects?q=name:$project" \
    --header "Authorization: Bearer $bearer_token")
  if [[ $(echo "$result" | jq -r .totalCount) -eq 0 ]]
  then
    >&2 echo "ERROR: No project found with name: $project"
    exit 1
  else
    echo "$result" | jq -r .items[0]._meta.href
  fi
}

function get_version_id {
  result=$(curl --silent --location --request GET "$project_api_url/versions?q=versionName:$version" \
    --header "Authorization: Bearer $bearer_token")
  if [[ $(echo "$result" | jq -r .totalCount) -eq 0 ]]

  then
    >&2 echo "ERROR: No version found with name: $version"
    exit 1
  else
    echo "$result" | jq -r .items[0]._meta.href
  fi
}

function create_sbom_report {
  dataraw="{\"reportFormat\": \"JSON\", \"reportType\" : \"SBOM\", \"sbomType\" : \"SPDX_22\"}"
  result=$(curl --silent --location --request POST "$version_api_url/sbom-reports" \
    --header "Authorization: Bearer $bearer_token" \
    --header 'Accept: application/json' \
    --header 'Content-Type: application/json' \
    --data-raw "$dataraw" )
  if [ "$result" != "" ]
  then
    >&2 echo "ERROR: error in creating SBOM report"
    >&2 echo $result
    exit 1
  fi
}

function get_report_id {
  report_status="IN_PROGRESS"
  max_retries=50
  retries=0

  while [ "$report_status" = "IN_PROGRESS" ]
  do
    ((retries++))
    if [ "$retries" -gt "$max_retries" ];
    then
      >&2 echo "ERROR: max retries reached"
      exit 1
    fi
    echo "| attempt $retries of $max_retries to get SPDX report"
    sleep 15
    result=$(curl --silent --location --request GET "$version_api_url/reports" \
      --header "Authorization: Bearer $bearer_token" \
      --header 'Content-Type: application/json')
    report_api_url=$(echo "$result" | jq -r '.items[0]._meta.href')
    report_status=$(echo "$result" | jq -r '.items[0].status')
    echo "| - report_status: $report_status"
  done

  if [ "$report_status" != "COMPLETED" ];
  then
    >&2 echo " ERROR: report_status is not COMPLETED, it is $report_status."
    exit 1
  fi
}

function download_sbom_report {
  curl --silent --location --request GET "$report_api_url/download.zip" \
    -o /tmp/reports/sbom.zip \
    --header "Authorization: Bearer $bearer_token" \
    --header 'Content-Type: application/zip'
}

function get_report_contents {
  curl --silent --location --request GET "$report_api_url/contents" \
    --header "Authorization: Bearer $bearer_token" \
    --header 'Content-Type: application/json' | jq -rc .reportContent[0].fileContent
}


#### Main program

show_banner

error=false

if [ -z "$1" ]
  then
    echo "ERROR: No blackduck-url supplied"
    error=true
fi

if [ -z "$2" ]
  then
    echo "ERROR: No blackduck-api-token supplied"
    error=true
fi

if [ -z "$3" ]
  then
    echo "ERROR: No project-name supplied"
    error=true
fi

if [ -z "$4" ]
  then
    echo "ERROR: No version-name supplied"
    error=true
fi

if [ $error == "true" ]
  then
    show_usage
    exit 1
fi

blackduck_url=$1
blackduck_token=$2
project=$3
version=$4

sbom_type="SPDX_22"

echo "+ getting bearer"
bearer_token=$(get_bearer)
echo "| got bearer"
echo

echo "+ getting project api base url"
project_api_url=$(get_project_id)
echo "| got project api base url: ${project_api_url}"
echo

echo "+ getting version api base url"
version_api_url=$(get_version_id)
echo "| got version api base url: ${version_api_url}"
echo

echo "+ creating SBOM report"
if [ "${NO_CREATE}" == true ]
then
  echo "| We're not creating a new report for the because of the secret environment variable NO_CREATE"
else
  create_sbom_report
  echo "| triggered creating SBOM report"
fi
echo

echo "+ getting SBOM report api base url"
get_report_id
echo "| got SBOM report status: ${report_status}"
echo "| got SBOM report api base url: ${report_api_url}"
echo

echo "+ getting SBOM report"
download_sbom_report
echo "| got SBOM report"
echo

echo "+ getting content information"
report_contents=$(get_report_contents)
echo "| got content information"
echo

echo "sbom-file=sbom.zip" >> $GITHUB_OUTPUT
echo "sbom-contents=${report_contents}" >> $GITHUB_OUTPUT
