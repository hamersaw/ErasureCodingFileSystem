A distributed file system implementation using erasure coding to guarantee accessability in the case of node failure

java -cp build/libs/ErasureCodingFileSystem.jar com.hamersaw.erasure_coding_file_system.Controller 15605
java -cp build/libs/ErasureCodingFileSystem.jar com.hamersaw.erasure_coding_file_system.ShardServer /tmp/ss1 localhost 15605 15606
java -cp build/libs/ErasureCodingFileSystem.jar:libs/JavaReedSolomon.jar com.hamersaw.erasure_coding_file_system.Client localhost 15605
