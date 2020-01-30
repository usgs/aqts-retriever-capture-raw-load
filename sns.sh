#!/bin/bash
# checks for an AWS::SNS::Topic resource that matches the sns variable
# tier is passed in as parameter #1
# first is creates the SNS resourceName
# then it greps for it in the sns list
# if nothing is returned then it attempts to create it
# note: I had to make an assumption about what is returned when a resource fails

# check for the resource
sns="IOW-RETRIEVER-CAPTURE-$1"
echo "checking if SNS exists: $sns"
exists=$(aws sns list-topics --region us-west-2 | grep $sns)

# if it missing then create it
if [[ $exists == *"IOW-RETRIEVER-CAPTURE"* ]]; then
  echo "SNS exists: $sns"
else
  echo "SNS missing and creation attempt: $sns"
  exists=$(aws sns create-topic --region us-west-2 --name $sns)
  if [[ $exists == *"IOW-RETRIEVER-CAPTURE"* ]]; then
    echo "SNS creation successful: $exists"
  fi
fi
