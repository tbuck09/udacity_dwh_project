# Data Warehousing with AWS
## Loading Music Data into AWS Redshift
As part of the **Data Engineering** nanodegree program through Udacity, this project seeks to capture the basics of using the AWS Redshift database and other AWS services.

## Overview
The data being modeled has come from a couple of different sources, both of which are meant to simulate the kind of data that a music streaming service such as Spotify might produce.

The files holding the data to be modeled are stored using AWS's S3 service. These files are loaded into staging tables in Redshift using a Python script and ETL is performed on these staging tables to put them into the appropriate star schema for further analysis.

## Instructions
As this is a project for a Udacity course, it is expected that the reviewers will know the configurations for setting this up.

**If you are a Udacity affiliate:** Skip to step 3. 

**If unaffiliated with Udacity:** Instructions are below. An Infrastructure-as-Code (IaC) option will be provided in the future. (Note: You will still need an AWS Key and Secret.)
### AWS Configurations
1. In order to run this script, you will need to have:
    1. An [AWS Account](https://console.aws.amazon.com/ "AWS Console").
    1. [An IAM user](https://console.aws.amazon.com/iam/home#/users$new?step=details "AWS IAM User: Create") and a [Role](https://console.aws.amazon.com/iam/home#/roles$new?step=permissions&selectedService=Redshift&selectedUseCase=Redshift "AWS Role: Create - Redshift Use Case") which are able to read files from S3 and have full access to Redshift.
        * Permissions
            * AmazonRedshiftFullAccess
            * AmazonS3ReadOnlyAccess
    1. [Security groups](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#CreateSecurityGroup: "AWS Security Group: Create") set up such that you can access the Redshift cluster.
        * Type: Custom TCP
        * Port Range: 5439
        * Source: Custom & 0.0.0.0/0
    1. An active AWS [Redshift cluster](https://us-west-2.console.aws.amazon.com/redshiftv2/home?region=us-west-2#create-cluster "AWS Redshift: Create").
        * Create this in the us-west-2 region, as this is the region where the data resides and will ensure processing is as fast as possible.
        * Cluster Identifier: Whatever you want
        * Node type: dc2.large
        * Nodes: 4
        * Database Name: Whatever you want
        * Database Port: 5439
        * Master user name: Whatever you want
        * Master user password: Whatever you want
        * Attach IAM Role
        * Attach Security Group
        * Make publicly accessible
        * Default settings for everything else
    1. **Wait** for the Redshift cluster to become active.
### Input Configurations - dwh.cfg
2. Enter your Redshift cluster configurations into the `dwh.cfg` file.
    * CLUSTER (Redshift)
        * Host (ex: 'dwh-project.fakefakefak3.us-west-2.redshift.amazonaws.com')
        * Database Name (ex: 'dev')
        * Database User (ex: awsuser)
        * Database Password (ex: 1234asdf)
        * Port of the connection (ex: 5439)
    * IAM_ROLE
        * Your ARN (ex: arn:aws:iam::123456789012:user/redshift_user)
### Create Tables
3. The `create_tables.py` file should be run in the console first in order to
    1. drop any tables which currently exist and
    2. create fresh tables which the data can be loaded into.
### ETL
4. Once the tables are created, run the `etl.py`.
### Delete Redshift Cluster
5. **In order to avoid charges!** Delete the Redshift cluster you have created.

## Technologies
### Data Simulation Services
* [Million Song Dataset](http://millionsongdataset.com/) for creating song/artist data and meta/data.
    > Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. The Million Song Dataset. In Proceedings of the 12th International Society for Music Information Retrieval Conference (ISMIR 2011), 2011.
* [eventsim](https://github.com/Interana/eventsim) for simulating user activity using data from the source above.

### Amazon Web Services
* S3
* Redshift

### Programming Language
* Python