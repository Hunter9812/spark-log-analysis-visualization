CREATE DATABASE maxwell;

CREATE USER 'maxwell'@'%' IDENTIFIED BY 'maxwell';
CREATE USER 'maxwell'@'localhost' IDENTIFIED BY 'maxwell';

GRANT ALL ON maxwell.* TO 'maxwell'@'%';
GRANT ALL ON maxwell.* TO 'maxwell'@'localhost';

GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'localhost';
