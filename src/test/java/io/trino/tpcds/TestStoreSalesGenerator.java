/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.tpcds;

import com.google.common.base.Stopwatch;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import io.trino.tpcds.Parallel.ChunkBoundaries;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.tpcds.GeneratorAssertions.assertPartialMD5;
import static io.trino.tpcds.Parallel.splitWork;
import static io.trino.tpcds.Session.getDefaultSession;
import static io.trino.tpcds.Table.STORE_SALES;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestStoreSalesGenerator
{
    private static final Session TEST_SESSION = getDefaultSession().withTable(STORE_SALES);

    // See the comment in CallCenterGeneratorTest for an explanation on the purpose of this test.
    @Test
    public void testScaleFactor0_01()
    {
        Session session = TEST_SESSION.withScale(0.01);
        assertPartialMD5(1, session.getScaling().getRowCount(STORE_SALES), STORE_SALES, session, "082d16bc89e2edcdda34b6b752ba13bd");
    }

    @Test
    public void testScaleFactor1()
    {
        Session session = TEST_SESSION.withScale(1);
        assertPartialMD5(1, session.getScaling().getRowCount(STORE_SALES), STORE_SALES, session, "f003b3810e042d6dd47f48506616d88d");
    }

    @Test
    public void testScaleFactor10()
    {
        Session session = TEST_SESSION.withScale(10).withParallelism(100).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "6d9e48c1c68357481c1726c4f3188ff0");

        session = session.withChunkNumber(10);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "1b1e82114c82251f635b0da34d94bec7");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "37d545933a5dc90262de4564bdf38a17");
    }

    @Test
    public void testScaleFactor100()
    {
        Session session = TEST_SESSION.withScale(100).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "b68babc4a3d38fc07f83b916d2f3efdb");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "190132ba69a76cfddbfb5569655eeaa0");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "77da04ae48f644889fed649011c86c05");
    }

    @Test
    public void testScaleFactor300()
    {
        Session session = TEST_SESSION.withScale(300).withParallelism(3000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "168f2a87c886a33d9a460a50ebbe8637");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "a3729fd4e5fc2e7db6e84f767f4585b4");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "cd8ae61701c8be67a27a806994eff227");
    }

    @Test
    public void testScaleFactor1000()
    {
        Session session = TEST_SESSION.withScale(1000).withParallelism(10000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "18eb0b9ee680cab4583a14d64cf373c3");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "69a52406a5f835afd3f01d38b8c4e6ef");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "f0ada0ccb0a7ae92f2cadad3dd0593d7");
    }

    @Test
    public void testScaleFactor3000()
    {
        Session session = TEST_SESSION.withScale(3000).withParallelism(30000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "d2d83b48ffae7b4577ff4b99abb6f7e6");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "90345a5ab207305e56c7b03d673abd34");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "94406945622849f3215572e2e0dbc222");
    }

    @Test
    public void testScaleFactor10000()
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(100000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "d9a9bce5e809651744a98d8fd0908019");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "f47b6b10044041dcf43c805790a3f7b2");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "b0ba7317a92c8ab4eaf0d9233ad37ecb");
    }

    @DataProvider(parallel = true)
    public Object[][] testScaleFactor10000ExtendedProvider()
    {
        return new Object[][] {
//                new Object[] {930, 123, "85a5093fe069f3a722ecc7d72f36c87e"},
//                new Object[] {930, 1, "cbc9984365102448d5b1ff0f62012622"},
                new Object[] {930, 272, "e1336aa1c1f52b6e040c67422955193b"},
//                new Object[] {930, 313, "28e4ebc2e751dd8303eaa9bb2222e404"},
//                new Object[] {930, 3, "dd90ea813e4ab4cc750673a8bc48b072"},
//                new Object[] {930, 777, "e1ffbb11578d8c4aa7f70438e48d140c"},
//                new Object[] {930, 7, "15f900e8a01bae6b441cfd70bf7d899a"},
//                new Object[] {930, 877, "0a16900b569fc4dca5f0d9519e0541f0"},
//                new Object[] {930, 929, "e4c8e5dd3e4840e88f63c1bd4a04c499"},
//                new Object[] {930, 930, "37f76ea69d9c314d38a6ef27c6f70c38"}
        };
    }

    @Test(dataProvider = "testScaleFactor10000ExtendedProvider", threadPoolSize = 10)
    public void testScaleFactor10000Extended(int parallelism, int chunk, String expected)
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(parallelism).withChunkNumber(chunk);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, expected);
    }

    @Test
    public void testChunkBoundaries()
    {

    }

    private static final File DSDGEN_DIRECTORY = new File("/home/andriirosa/Downloads/DSGen-software-code-3.2.0rc1/tools");

    @DataProvider(parallel = true)
    public Object[][] testGeneratedProvider()
    {
        int scaleFactor = 10000;
        int parallelism = 930;
        int step = 271;
        List<Object[]> result = new ArrayList<>();
//        for (int chunk = 1; chunk <= parallelism; chunk += step) {
//            result.add(new Object[] {scaleFactor, parallelism, chunk});
//        }
        result.add(new Object[] {scaleFactor, parallelism, 272});
        return result.toArray(new Object[][] {{}});
    }

    @Test(dataProvider = "testGeneratedProvider", threadPoolSize = 10)
    public void testGenerated(int scaleFactor, int parallelism, int chunk)
            throws Exception
    {
        File dir = Files.createTempDir();
        dir.deleteOnExit();

        Stopwatch stopwatch = Stopwatch.createStarted();
        ProcessBuilder processBuilder = new ProcessBuilder(
                "./dsdgen",
                "-table", "store_sales",
                "-scale", Integer.toString(scaleFactor),
                "-parallel", Integer.toString(parallelism),
                "-child", Integer.toString(chunk),
                "-dir", dir.getAbsolutePath());
        processBuilder.directory(DSDGEN_DIRECTORY);
        processBuilder.redirectOutput(INHERIT);
        processBuilder.redirectError(INHERIT);
        Process p = processBuilder.start();
        int exitCode = p.waitFor();
        assertEquals(exitCode, 0);
        System.out.printf("dsdgen completed in %s for chunk %s\n", stopwatch, chunk);

        File file = new File(format("%s/store_sales_%s_%s.dat", dir.getAbsolutePath(), chunk, parallelism));

        stopwatch.reset().start();
        HashCode hash = Files.asByteSource(file).hash(Hashing.md5());
        String expectedHash = hash.toString();
        System.out.printf("md5 hash computed in %s for chunk %s\n", stopwatch, chunk);

        stopwatch.reset().start();
        long numberOfRows = Files.asCharSource(file, UTF_8).lines().count();
        System.out.printf("number of rows [%s] computed in %s for chunk %s\n", numberOfRows, stopwatch, chunk);

        MoreFiles.deleteRecursively(dir.toPath(), ALLOW_INSECURE);

        stopwatch.reset().start();
        Session session = TEST_SESSION.withScale(scaleFactor).withParallelism(parallelism).withChunkNumber(chunk);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, expectedHash);
        System.out.printf("actual md5 hash generated in %s for chunk %s\n", stopwatch, chunk);
    }

    @Test
    public void testScaleFactor30000()
    {
        Session session = TEST_SESSION.withScale(30000).withParallelism(300000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "94f217e231e0cb3ce29c94d1449090a4");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "ff3d29f89e410d87ffd13840196886f6");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "0055c7a18a24406897cf39d4f0769249");
    }

    @Test
    public void testScaleFactor100000()
    {
        Session session = TEST_SESSION.withScale(100000).withParallelism(1000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "3531c27fdd43884a883d42dedc737be4");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "de5bd07e97bbabbb0092e4a3eb619b20");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "29dd85694f7ae1f0b9718d21c1e9fa9b");
    }

    @Test
    public void testUndefinedScale()
    {
        Session session = TEST_SESSION.withScale(15).withParallelism(150).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "8f4cba435ce81902757e7ab198cbd66e");

        session = session.withChunkNumber(10);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "2ced7f5a3093797aa6977d31bc1b76ce");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_SALES, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_SALES, session, "d535f06ce741f45b4dd9fceb70d6ccf4");
    }
}
