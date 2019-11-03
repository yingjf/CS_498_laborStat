#!/bin/bash
rm -rf ./build/* ./LaborStats.jar
hadoop com.sun.tools.javac.Main LaborStats.java -d build
jar -cvf LaborStats.jar -C build/ ./
hadoop fs -rm -r -f /project/output