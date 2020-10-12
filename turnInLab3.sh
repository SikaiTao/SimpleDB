#!/bin/sh
git tag -d lab3submit
git push origin :refs/tags/lab3submit
git add --all .
git commit -a -m 'Lab 3--All Finished'
git tag -a lab3submit -m 'submit lab 3'
git push origin main --tags
