/*-
 * ========================LICENSE_START=================================
 * flyway-core
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
 * ========================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.flywaydb.core.internal.resolver;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.resource.LoadableResource;
import org.flywaydb.core.internal.util.BomFilter;
import org.flywaydb.core.internal.util.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ChecksumCalculator {
    /**
     * Calculates the checksum of these resources. The checksum is encoding and line-ending independent.
     *
     * @return The crc-32 checksum of the bytes.
     */
    public static int calculate(LoadableResource... loadableResources) {
        int checksum;




            checksum = calculateChecksumForResource(loadableResources[0]);












        return checksum;
    }

    private static int calculateChecksumForResource(LoadableResource resource) {
        final CRC32 crc32 = new CRC32();

        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(resource.read(), 4096);
            String line = bufferedReader.readLine();

            if (line != null) {
                line = BomFilter.FilterBomFromString(line);
                do {
                    //noinspection Since15
                    crc32.update(line.getBytes(StandardCharsets.UTF_8));
                } while ((line = bufferedReader.readLine()) != null);
            }
        } catch (IOException e) {
            throw new FlywayException("Unable to calculate checksum of " + resource.getFilename() + "\n" +
                                      "Please ensure you have configured the correct file encoding with 'flyway.encoding' " +
                                      "or enable 'flyway.detectEncoding' to let Flyway detect it for you", e);
        } finally {
            IOUtils.close(bufferedReader);
        }

        return (int) crc32.getValue();
    }











}
