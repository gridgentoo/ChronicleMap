/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eg;

import net.openhft.chronicle.wire.TextWire;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.IByteBufferBytes;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by Rob Austin
 */
public class MapWireProtocol {


    @Test
    public void test() throws Exception {

        IByteBufferBytes bytes = ByteBufferBytes.wrap(ByteBuffer.wrap(new byte[1024]));
        new TextWire(bytes).write(() -> "type").text("stateless-map");
        bytes.flip();

        System.out.println("XXX" + AbstractBytes.toHex(bytes));

    }
}
