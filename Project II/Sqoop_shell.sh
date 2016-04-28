sqoop export --connect jdbc:mysql://localhost/ProjectI --username 'root' -P --table 'Progress' --export-dir '/pigout' --input-fields-terminated-by '\t' -m 1

sqoop export --connect jdbc:mysql://localhost/ProjectI --username 'root' -P --table 'Problem_Statement1' --export-dir '/Problem_Statement1' --input-fields-terminated-by ',' -m 1

sqoop export --connect jdbc:mysql://localhost/ProjectI --username 'root' -P --table 'Problem_Statement2' --export-dir '/Problem_Statement2' --input-fields-terminated-by ',' -m 1

