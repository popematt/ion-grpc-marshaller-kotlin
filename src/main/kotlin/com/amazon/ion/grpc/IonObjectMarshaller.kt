package com.amazon.ion.grpc

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.ion.IonGenerator
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper
import com.fasterxml.jackson.dataformat.ion.IonParser
import com.fasterxml.jackson.dataformat.ion.jsr310.IonJavaTimeModule
import io.grpc.MethodDescriptor
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream


class IonObjectMarshaller(
    val mapper: IonObjectMapper = IonObjectMapper.builderForBinaryWriters()
        .addModule(IonJavaTimeModule()) //Disable writing dates as numeric timestamp values to allow writing as Ion timestamp values.
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .enable(IonParser.Feature.USE_NATIVE_TYPE_ID)
        .enable(IonGenerator.Feature.USE_NATIVE_TYPE_ID)
        .build()
) {

    fun <T> marshallerFor(clazz: Class<T>): MethodDescriptor.Marshaller<T> {
        return object: MethodDescriptor.Marshaller<T> {
            override fun stream(value: T): InputStream {
                return ByteArrayOutputStream().use {
                    mapper.writeValue(it, value)
                    ByteArrayInputStream(it.toByteArray())
                }
            }

            override fun parse(stream: InputStream): T {
                return mapper.readValue(stream, clazz)
            }
        }
    }
}
