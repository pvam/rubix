/**
 * Copyright (c) 2016. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.core;

import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.RandomAccessFile;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 4/1/16.
 *
 * This chain reads from Remote and stores one copy in cache
 */
public class RemoteReadRequestChain
        extends ReadRequestChain
{
    final FSDataInputStream inputStream;
    final String localFilename;

    private int blockSize = 0;
    private long warmupPenalty = 0;
    private int readBackendBytes = 0;
    private int readActualBytes = 0;

    private static final Log log = LogFactory.getLog(RemoteReadRequestChain.class);

    public RemoteReadRequestChain(FSDataInputStream inputStream, String localFile, int blockSize)
    {
        this.inputStream = inputStream;
        this.localFilename = localFile;
        this.blockSize = blockSize;
    }

    public Integer call()
            throws IOException
    {
        Thread.currentThread().setName(threadName);
        checkState(isLocked, "Trying to execute Chain without locking");

        if (readRequests.size() == 0) {
            return 0;
        }

        RandomAccessFile localFile = null;

        try {
            localFile = new RandomAccessFile(localFilename, "rw");
            for (ReadRequest readRequest : readRequests) {
                log.debug(String.format("Executing ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
                inputStream.seek(readRequest.getActualReadStart());
                localFile.seek(readRequest.getActualReadStart());
                readBackendBytes += readRequest.getBackendReadLength();
                log.debug(String.format("Trying to Read %d bytes into destination buffer", readRequest.getActualReadLength()));
                int readBytes = readAndCopy(readRequest.getDestBuffer(), readRequest.destBufferOffset, readRequest.getActualReadLength());
                readActualBytes += readBytes;
                log.debug(String.format("Read %d bytes into destination buffer", readBytes));

                long start = System.nanoTime();
                long cacheReadStartBlock = getCacheReadStartBlock(readRequest);
                long cacheReadEndBlock = getCacheReadEndBlock(readRequest);
                if (cacheReadStartBlock <= cacheReadEndBlock) {
                    int beginOffset = (int) (cacheReadStartBlock * blockSize - readRequest.getActualReadStart());
                    int endOffset = (int) (readRequest.getActualReadEnd() - (cacheReadEndBlock + 1) * blockSize);
                    int cacheReadLength = readRequest.getActualReadLength() - beginOffset - endOffset;
                    localFile.write(readRequest.getDestBuffer(), readRequest.destBufferOffset + beginOffset, cacheReadLength);
                    warmupPenalty += System.nanoTime() - start;
                    log.debug(String.format("Read %d bytes into cache", cacheReadLength));
                }
            }
        }
        finally {
            if (localFile != null) {
                localFile.close();
            }
        }
        log.info(String.format("Read %d bytes from remote file, added %d to destination buffer", readBackendBytes, readActualBytes));
        return readActualBytes;
    }

    private int readAndCopy(byte[] destBuffer, int destBufferOffset, int length)
            throws IOException
    {
        int nread = 0;
        while (nread < length) {
            int nbytes = inputStream.read(destBuffer, destBufferOffset + nread, length - nread);
            if (nbytes < 0) {
                return nread;
            }
            nread += nbytes;
        }
        return nread;
    }

    public ReadRequestChainStats getStats()
    {
        return new ReadRequestChainStats()
                .setReadActualBytes(readActualBytes)
                .setReadBackendBytes(readBackendBytes)
                .setWarmupPenalty(warmupPenalty)
                .setRemoteReads(requests);
    }

    @Override
    public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
    {
        try {
            BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
            RetryingBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf);
            for (ReadRequest readRequest : readRequests) {
                long cacheBeginBlock = getCacheReadStartBlock(readRequest);
                long cacheEndBlock = getCacheReadEndBlock(readRequest) + 1;
                System.out.printf("Cache block, begin %d , end %d\n", cacheBeginBlock, cacheEndBlock);
                //Check whether there is at least one block that can be cached
                if (cacheBeginBlock < cacheEndBlock) {
                    System.out.println("Caching some part of data");
                    client.setAllCached(remotePath, fileSize, lastModified, cacheBeginBlock, cacheEndBlock);
                }
            }
            client.close();
        }
        catch (Exception e) {
            log.info("Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
        }
    }

    private long getCacheReadStartBlock(ReadRequest readRequest)
    {
        return toBlock(readRequest.actualReadStart + blockSize - 1);
    }

    private long getCacheReadEndBlock(ReadRequest readRequest)
    {
        return toBlock(readRequest.actualReadEnd - blockSize + 1);
    }

    private long toBlock(long pos)
    {
        return pos / blockSize;
    }
}
