#!/bin/bash
# This scripts generates the test data for intergration tests
# See main_test.go for detailed description of the contents.

RANDOM_SEED=12368

function create_file() {
    FILE_PATH=$1
    DIFF=$((SIZE_MAX-SIZE_MIN))
    SIZE=$(((RANDOM % DIFF) + SIZE_MIN))
    head -c $SIZE </dev/urandom > $FILE_PATH
}

if [ "$#" -ne 2 ]; then
    echo "Usage: " $0 " <minimum file size> <maximum file size>"
    exit 1
fi

SIZE_MIN=$1
SIZE_MAX=$2
RANDOM=$RANDOM_SEED

mkdir -p integration_test_data/folder1/family
mkdir -p integration_test_data/folder1/friends

mkdir -p integration_test_data/folder2/family
mkdir -p integration_test_data/folder2/friends

mkdir -p integration_test_data/folder3/family
mkdir -p integration_test_data/folder3/friends

mkdir -p integration_test_data/folder4/holiday/public

create_file integration_test_data/folder1/family/mom.jpg
create_file integration_test_data/folder1/family/dad.jpg
create_file integration_test_data/folder1/family/sis.jpg
create_file integration_test_data/folder1/friends/kara.jpg
create_file integration_test_data/folder1/friends/conor.jpg
create_file integration_test_data/folder1/friends/markus.jpg
create_file integration_test_data/folder1/funny.png

cp integration_test_data/folder1/family/mom.jpg integration_test_data/folder2/family
cp integration_test_data/folder1/family/dad.jpg integration_test_data/folder2/family/daddy.jpg
create_file integration_test_data/folder2/friends/tom.jpg
create_file integration_test_data/folder2/friends/jerry.jpg
cp integration_test_data/folder1/friends/markus.jpg integration_test_data/folder2/friends

cp integration_test_data/folder1/family/mom.jpg integration_test_data/folder3/family
cp integration_test_data/folder1/family/sis.jpg integration_test_data/folder3/family
cp integration_test_data/folder1/friends/conor.jpg integration_test_data/folder3/friends
cp integration_test_data/folder1/friends/markus.jpg integration_test_data/folder3/friends
cp integration_test_data/folder1/funny.png integration_test_data/folder3/

create_file integration_test_data/folder4/holiday/view1.jpg
create_file integration_test_data/folder4/holiday/view2.jpg
create_file integration_test_data/folder4/holiday/view3.jpg
cp integration_test_data/folder4/holiday/view1.jpg integration_test_data/folder4/holiday/public
cp integration_test_data/folder4/holiday/view2.jpg integration_test_data/folder4/holiday/public
cp integration_test_data/folder4/holiday/view1.jpg integration_test_data/folder4/