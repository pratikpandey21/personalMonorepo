```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
 contract/log.proto
```

- Why reading from proto file worked
  https://pandulaofficial.medium.com/reading-and-writing-multiple-records-to-a-file-with-protobuf-format-using-go-abde652c81e9

- Things to add -
  - Benchmarking
  - Checkpointing
  - Replicated Log
    - Will help build master slave
