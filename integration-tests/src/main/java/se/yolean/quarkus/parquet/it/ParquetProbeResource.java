package se.yolean.quarkus.parquet.it;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

@jakarta.ws.rs.Path("/parquet/probe")
public class ParquetProbeResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String probeParquetRoundTrip() throws IOException {
        Path parquetFile = Files.createTempFile("quarkus-parquet-probe", ".parquet");
        Files.deleteIfExists(parquetFile);
        try {
            MessageType schema = Types.buildMessage()
                    .required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("name")
                    .required(PrimitiveTypeName.INT32)
                    .named("age")
                    .named("Person");

            int nameIndex = schema.getFieldIndex("name");
            int ageIndex = schema.getFieldIndex("age");

            try (ParquetWriter<Group> writer = new SimpleGroupWriterBuilder(new NioOutputFile(parquetFile), schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()) {
                Group alice = new SimpleGroup(schema);
                alice.add(nameIndex, "Alice");
                alice.add(ageIndex, 34);

                Group bob = new SimpleGroup(schema);
                bob.add(nameIndex, "Bob");
                bob.add(ageIndex, 28);

                writer.write(alice);
                writer.write(bob);
            }

            List<Group> rows = readAllGroups(parquetFile);
            return rows.stream()
                    .map(group -> group.getBinary(nameIndex, 0).toStringUsingUTF8() + ":" + group.getInteger(ageIndex, 0))
                    .collect(Collectors.joining(","));
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    private static List<Group> readAllGroups(Path path) throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(new NioInputFile(path))) {
            MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
            ColumnIOFactory columnIOFactory = new ColumnIOFactory();
            List<Group> rows = new ArrayList<>();
            var pages = fileReader.readNextRowGroup();
            while (pages != null) {
                long rowCount = pages.getRowCount();
                RecordReader<Group> recordReader = columnIOFactory.getColumnIO(fileSchema)
                        .getRecordReader(pages, new GroupRecordConverter(fileSchema));
                for (long i = 0; i < rowCount; i++) {
                    rows.add(recordReader.read());
                }
                pages = fileReader.readNextRowGroup();
            }
            return rows;
        }
    }

    private static final class SimpleGroupWriterBuilder extends ParquetWriter.Builder<Group, SimpleGroupWriterBuilder> {

        private final MessageType schema;

        private SimpleGroupWriterBuilder(OutputFile file, MessageType schema) {
            super(file);
            this.schema = schema;
        }

        @Override
        protected SimpleGroupWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(ParquetConfiguration configuration) {
            configuration.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
            return new GroupWriteSupport();
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(Configuration conf) {
            GroupWriteSupport.setSchema(schema, conf);
            return new GroupWriteSupport();
        }
    }

    private static final class NioOutputFile implements OutputFile {

        private final Path path;

        private NioOutputFile(Path path) {
            this.path = path;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return new NioPositionOutputStream(newByteChannel(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return new NioPositionOutputStream(newByteChannel(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return ParquetWriter.DEFAULT_BLOCK_SIZE;
        }

        private SeekableByteChannel newByteChannel(StandardOpenOption... options) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            return Files.newByteChannel(path, options);
        }
    }

    private static final class NioInputFile implements InputFile {

        private final Path path;

        private NioInputFile(Path path) {
            this.path = path;
        }

        @Override
        public long getLength() throws IOException {
            return Files.size(path);
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new NioSeekableInputStream(Files.newByteChannel(path, StandardOpenOption.READ));
        }
    }

    private static final class NioPositionOutputStream extends PositionOutputStream {

        private final SeekableByteChannel channel;

        private NioPositionOutputStream(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void write(int b) throws IOException {
            channel.write(ByteBuffer.wrap(new byte[] { (byte) b }));
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len > 0) {
                channel.write(ByteBuffer.wrap(b, off, len));
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    private static final class NioSeekableInputStream extends DelegatingSeekableInputStream {

        private final SeekableByteChannel channel;

        private NioSeekableInputStream(SeekableByteChannel channel) {
            super(Channels.newInputStream(channel));
            this.channel = channel;
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            channel.position(newPos);
        }

        @Override
        public int read() throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            int read = channel.read(buffer);
            if (read <= 0) {
                return read;
            }
            buffer.flip();
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            return channel.read(ByteBuffer.wrap(b, off, len));
        }

        @Override
        public void close() throws IOException {
            super.close();
            channel.close();
        }
    }
}
