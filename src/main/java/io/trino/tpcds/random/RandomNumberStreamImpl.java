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

package io.trino.tpcds.random;

import static com.google.common.base.Preconditions.checkArgument;

public class RandomNumberStreamImpl
        implements RandomNumberStream
{
    private static final int DEFAULT_SEED_BASE = 19620718;
    private static final int MULTIPLIER = 16807;
    private static final int QUOTIENT = 127773;   // the quotient MAX_INT / MULTIPLIER
    private static final int REMAINDER = 2836;    // the remainder MAX_INT % MULTIPLIER

    private long seed;
    private final long initialSeed;
    private int seedsUsed;
    private final int seedsPerRow;

    public RandomNumberStreamImpl(int seedsPerRow)
    {
        checkArgument(seedsPerRow >= 0, "seedsPerRow must be >=0");
        this.initialSeed = 3;
        this.seed = 3;
        this.seedsPerRow = seedsPerRow;
    }

    public RandomNumberStreamImpl(int globalColumnNumber, int seedsPerRow)
    {
        this(globalColumnNumber, DEFAULT_SEED_BASE, seedsPerRow);
    }

    public RandomNumberStreamImpl(int globalColumnNumber, int seedBase, int seedsPerRow)
    {
        checkArgument(seedsPerRow >= 0, "seedsPerRow must be >=0");
        this.initialSeed = seedBase + globalColumnNumber * (Integer.MAX_VALUE / 799);
        this.seed = initialSeed;
        this.seedsPerRow = seedsPerRow;
    }

    @Override
    // https://en.wikipedia.org/wiki/Lehmer_random_number_generator
    public long nextRandom()
    {
        long nextSeed = seed;
        long divisionResult = nextSeed / QUOTIENT;
        long modResult = nextSeed % QUOTIENT;
        nextSeed = MULTIPLIER * modResult - divisionResult * REMAINDER;
        if (nextSeed < 0) {
            nextSeed += Integer.MAX_VALUE;
        }

        seed = nextSeed;
        seedsUsed += 1;
        return seed;
    }

    @Override
    public double nextRandomDouble()
    {
        return (double) this.nextRandom() / (double) Integer.MAX_VALUE;
    }

    @Override
    public void skipRows(long numberOfRows)
    {
        long numberOfValuesToSkip = numberOfRows * seedsPerRow;
        long nextSeed = initialSeed;
        long multiplier = MULTIPLIER;
        while (numberOfValuesToSkip > 0) {
            if (numberOfValuesToSkip % 2 != 0) { // n is odd
                nextSeed = (multiplier * nextSeed) % Integer.MAX_VALUE;
            }
            numberOfValuesToSkip = numberOfValuesToSkip / 2;
            multiplier = (multiplier * multiplier) % Integer.MAX_VALUE;
        }
        seed = nextSeed;
        seedsUsed = 0;
    }

    @Override
    public void resetSeed()
    {
        seed = initialSeed;
        seedsUsed = 0;
    }

    @Override
    public int getSeedsUsed()
    {
        return seedsUsed;
    }

    @Override
    public void resetSeedsUsed()
    {
        seedsUsed = 0;
    }

    @Override
    public int getSeedsPerRow()
    {
        return seedsPerRow;
    }
}
