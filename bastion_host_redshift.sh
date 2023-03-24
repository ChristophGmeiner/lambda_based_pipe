#!/bin/bash

ssh -i "bastion-host.pem" -f -N -l ec2-user -L 5555:test-rs-serverless-workgroup-02.120327452865.eu-central-1.redshift-serverless.amazonaws.com:5439 18.198.6.174