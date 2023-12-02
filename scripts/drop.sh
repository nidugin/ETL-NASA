#!/bin/sh
hive DROP DATABASE IF EXISTS nikita_base_test CASCADE;
hive DROP DATABASE IF EXISTS nikita_analytical_test CASCADE;
hive DROP DATABASE IF EXISTS nikita_error_test CASCADE;