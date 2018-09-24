package org.shirdrn.flink.broadcaststate;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class EvaluatedResultSchema implements DeserializationSchema<EvaluatedResult>,
    SerializationSchema<EvaluatedResult> {

  @Override
  public byte[] serialize(EvaluatedResult result) {
    return result.toJSONString().getBytes();
  }

  @Override
  public EvaluatedResult deserialize(byte[] message) throws IOException {
    return EvaluatedResult.fromString(new String(message));
  }

  @Override
  public boolean isEndOfStream(EvaluatedResult nextElement) {
    return false;
  }

  @Override
  public TypeInformation<EvaluatedResult> getProducedType() {
    return TypeInformation.of(EvaluatedResult.class);
  }
}
