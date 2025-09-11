ARROW_CFLAGS = $(shell pkg-config --cflags arrow-glib parquet-glib)
ARROW_LIBS = $(shell pkg-config --libs arrow-glib parquet-glib)

SOURCES = src/blast.c src/dbf_reader.c src/parquet_write.c src/main.c
TARGET = dbc2parquet

INPUT_FILE = ERSC2504.dbc
OUTPUT_FILE = ERSC2504.parquet

$(TARGET): $(SOURCES) src/blast.h src/dbf_reader.h src/parquet_write.h
	cc $(ARROW_CFLAGS)  -O2 -o $(TARGET) $(SOURCES) $(ARROW_LIBS)

test: $(TARGET)
	./$(TARGET) $(INPUT_FILE) $(OUTPUT_FILE)

clean:
	rm -f $(TARGET) *.o

.PHONY: test clean