#nikita_base_test
hive -hiveconf database=nikita_base_test -f /home/nikita_roldugins/scripts/database.hql
hive -hiveconf database=nikita_base_test -hiveconf table=apod -hiveconf location="/interns/test/base/apod_nikita_test/apod_nikita_base/" -f /home/nikita_roldugins/scripts/base/table_apod.hql
hive -hiveconf database=nikita_base_test -hiveconf table=neo -hiveconf location="/interns/test/base/neo_nikita_test/neo_nikita_base/" -f /home/nikita_roldugins/scripts/base/table_neo.hql


#nikita_analytical_test
hive -hiveconf database=nikita_analytical_test -f /home/nikita_roldugins/scripts/database.hql
hive -hiveconf database=nikita_analytical_test -hiveconf table=apod -hiveconf location="/interns/test/analytical/apod_nikita_test/apod_nikita_analytical/" -f /home/nikita_roldugins/scripts/analytical/table_apod.hql
hive -hiveconf database=nikita_analytical_test -hiveconf table=v_apod_dashboard_las_5_days -hiveconf location="/interns/test/analytical/apod_nikita_test/apod_nikita_analytical/" -f /home/nikita_roldugins/scripts/analytical/table_v_apod_dashboard_las_5_days.hql
hive -hiveconf database=nikita_analytical_test -hiveconf table=neo -hiveconf location="/interns/test/analytical/neo_nikita_test/neo_nikita_analytical/" -f /home/nikita_roldugins/scripts/analytical/table_neo.hql
hive -hiveconf database=nikita_analytical_test -hiveconf table=neo_hazard -hiveconf location="/interns/test/analytical/neo_nikita_test/neo_nikita_analytical/" -f /home/nikita_roldugins/scripts/analytical/table_neo_hazard.hql
hive -hiveconf database=nikita_analytical_test -hiveconf table=v_neo_hazard_dashboard_5 -hiveconf location="/interns/test/analytical/neo_nikita_test/neo_nikita_analytical/" -f /home/nikita_roldugins/scripts/analytical/table_v_neo_hazard_dashboard_5.hql


# nikita_error_test
hive -hiveconf database=nikita_error_test -f /home/nikita_roldugins/scripts/database.hql
hive -hiveconf database=nikita_error_test -hiveconf table=error_log -hiveconf location="/interns/test/error/nikita_log/" -f /home/nikita_roldugins/scripts/error/table_error.hql