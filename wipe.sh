export PGPASSWORD="password"
psql -h db -U postgres dejavu <<EOF
\dt
DROP TABLE IF EXISTS fingerprints;
DROP TABLE IF EXISTS songs;
EOF