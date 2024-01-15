package com.keks.kv_storage.io;

import com.keks.kv_storage.ex.KVStoreIOException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;


public class FileUtils {

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void emptyDirectory(File directoryToEmpty) {
        File[] allContents = directoryToEmpty.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
    }

    public static void copyFolder(File srcFile, File destFile) throws IOException {
        Path src = Path.of(srcFile.getAbsolutePath());
        Path dest = Path.of(destFile.getAbsolutePath());
        copyFolder(src, dest);
    }
    public static void copyFolder(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
        }
    }

    private static void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Recursively calculates directory size.
     */
    public static long getDirectorySizeInBytes(final Path path) {
        try {
            final AtomicLong result = new AtomicLong(0L);
            Files.walkFileTree(
                    path,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(
                                final Path file,
                                final BasicFileAttributes attrs) {
                            result.addAndGet(attrs.size());
                            return FileVisitResult.CONTINUE;
                        }
                    });
            return result.get();
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot get directory size " + path.toFile().getAbsolutePath(), e);
        }
    }

}
