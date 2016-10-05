#!/bin/sh

gox --output "bin/{{.OS}}-{{.Arch}}/{{.Dir}}" &&\
find bin -type f -print0 | xargs -0 -P4 -I{} upx --best --brute {}	
