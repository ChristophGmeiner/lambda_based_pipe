#!/bin/bash

ssh -i bastion-host.pem -4 -N -L 5555:test-rds-cluster01.cluster-cfv4eklkdk8x.eu-central-1.rds.amazonaws.com:5432 ec2-user@52.29.90.136