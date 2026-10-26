-- Läuft einmalig beim ersten Start, nach CREATE EXTENSION kafgres (01-...).
CREATE DATABASE cibseven;

SELECT kafgres_create_topic('kyc.approval.requested', 1);
SELECT kafgres_create_topic('kyc.approval.approved', 1);
SELECT kafgres_create_topic('kyc.approval.rejected', 1);
