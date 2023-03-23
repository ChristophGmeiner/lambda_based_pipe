#!/bin/bash

ssh -i "bastion-host.pem" -f -N -l ec2-user -L 5553:test-rds-cluster01.cluster-cfv4eklkdk8x.eu-central-1.rds.amazonaws.com:5432 18.198.6.174