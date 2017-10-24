path "sys/wrapping/unwrap" {
    capabilities = ["update"]
}

path "database/config/*" {
  capabilities = ["read", "write", "list"]
}

#the path is the hostname/port. key-value stores is db_name:[db_name], host:, port:, username:, password
path "secret/databases/services/postgres/*" {
  capabilities = ["read", "list"]
}

path "transit/datakey/wrapped/*" {
  capabilities = ["read"]
}

path "transit/datakey/plaintext/*" {
  capabilities = ["read"]
}
