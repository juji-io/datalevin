package datalevin.ni;

import static java.util.Objects.requireNonNull;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.lang.Thread.currentThread;
import static java.util.Locale.ENGLISH;

import com.oracle.svm.core.SubstrateOptions;
import com.oracle.svm.core.option.OptionUtils;
import com.oracle.svm.core.c.ProjectHeaderFile;

import org.graalvm.nativeimage.c.CContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


    /**
    * Sets up the context required for interacting with native library.
    */
public final class Directives implements CContext.Directives {
    static {
        final String extractEnv = getenv("DTLV_LIB_EXTRACT_DIR");
        final String defaultCLibPath =
            OptionUtils
            .flatten(",", SubstrateOptions.CLibraryPath.getValue())
            .get(0);
        final String EXTRACT_DIR =
            extractEnv == null ? defaultCLibPath : extractEnv;

        final String arch = getProperty("os.arch");
        final boolean arch64 = "x64".equals(arch) || "amd64".equals(arch)
            || "x86_64".equals(arch);

        final String os = getProperty("os.name");
        final boolean linux = os.toLowerCase(ENGLISH).startsWith("linux");
        final boolean osx = os.startsWith("Mac OS X");
        final boolean windows = os.startsWith("Windows");

        final String dtlvLibName, lmdbLibName, myPlatform;

        if (arch64 && linux) {
            dtlvLibName = "libdtlv.a";
            lmdbLibName = "liblmdb.a";
            myPlatform = "ubuntu-latest-amd64";
        } else if (arch64 && osx) {
            dtlvLibName = "libdtlv.a";
            lmdbLibName = "liblmdb.a";
            myPlatform = "macos-latest-amd64";
        }
        else if (arch64 && windows) {
            dtlvLibName = "dtlv.lib";
            lmdbLibName = "lmdb.lib";
            myPlatform = "windows-amd64";
        } else {
            throw new IllegalStateException("Unsupported platform: "
                                            + os + " on " + arch);
        }

        final String dtlvHeaderName = "dtlv.h";

        final String lmdbHeaderName = "lmdb.h";
        final String lmdbHeaderPath = "lmdb/libraries/liblmdb";
        final String lmdbHeaderDir = Paths.get(lmdbHeaderPath).toString();

        final File dir = new File(EXTRACT_DIR);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalStateException("Invalid extraction directory "
                                            + dir);
        }

        final String lmdbHeaderAbsDir = EXTRACT_DIR
            + File.separator + lmdbHeaderDir;

        try {
            Path path = Paths.get(lmdbHeaderAbsDir);
            Files.createDirectories(path);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to create directory "
                                            + lmdbHeaderAbsDir);
        }

        extract(EXTRACT_DIR, myPlatform,
                lmdbHeaderPath + "/" + lmdbHeaderName);
        extract(EXTRACT_DIR, myPlatform, dtlvHeaderName);
        extract(EXTRACT_DIR, myPlatform, dtlvLibName);
        extract(EXTRACT_DIR, myPlatform, lmdbLibName);
        System.out.println("Extraction successful!");
    }

    private static void extract(final String parent,
                                final String platform,
                                final String name) {
        try {
            final String filename = Paths.get(name).toString();
            final File file = new File(parent, filename);
            file.deleteOnExit();

            final ClassLoader cl = currentThread().getContextClassLoader();

            try (InputStream in = cl.getResourceAsStream("dtlvnative/"
                                                            + platform + "/"
                                                            + name);
                    OutputStream out = Files.newOutputStream(file.toPath())) {
                requireNonNull(in, "Classpath resource not found");
                int bytes;
                final byte[] buffer = new byte[4096];
                while (-1 != (bytes = in.read(buffer))) {
                    out.write(buffer, 0, bytes);
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to extract " + name
                                            + " in " + parent);
        }
    }

    @Override
    public List<String> getHeaderFiles() {
        return Collections
            .singletonList(ProjectHeaderFile.resolve("datalevin.ni",
                                                        "dtlv.h"));
    }

    @Override
    public List<String> getLibraries() {
        return Arrays.asList("lmdb", "dtlv");
    }
}
