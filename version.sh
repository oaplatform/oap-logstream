#!/bin/bash
# It helps to obtain the project version for TeamCity builds.
# It works for master and feature branches.
#
# Version example for master branch: 17.8.3.0-128
# 17.8.3.0 - project version (17 - java version, 8 - major project version, 3 - minor project version, 0 - patch version)
# 128 - teamcity build number
# Version example for the feature branch: 17.10.0.0-oap-nio-1134
# 17.10.0.0 - project version
# oap-nio - branch name (absent for master)
# 1134 - build number

set -x

BUILD_COUNTER=$1
PROJECT_NAME=$2
BRANCH_NAME=$3

VERSION_XENOSS=$(grep -oP 'project\.version\>\K[^<]*' pom.xml)

if [ "$BRANCH_NAME" == "master" ] || [ "$BRANCH_NAME" == "" ] || [ "$BRANCH_NAME" == "refs/heads/master" ]; then
  VERSION_BRANCH=""
  MAVEN_BUILD_COUNTER=""
else
  VERSION_BRANCH="-${BRANCH_NAME}"
  MAVEN_BUILD_COUNTER="-${BUILD_COUNTER}"
fi

set +x

#project name
echo "##teamcity[setParameter name='oap.project.name' value='${PROJECT_NAME,,}']"

#maven master
echo "##teamcity[setParameter name='${PROJECT_NAME,,}.project.version' value='${VERSION_XENOSS}']"

#maven branch
echo "##teamcity[setParameter name='oap.project.version.branch' value='${VERSION_XENOSS}${VERSION_BRANCH}${MAVEN_BUILD_COUNTER}']"

#teamcity
set +x

echo "##teamcity[buildNumber '${VERSION_XENOSS}${VERSION_BRANCH}-${BUILD_COUNTER}']"
