package com.amazon.ion.grpc

import com.amazon.ion.IonSystem
import com.amazon.ion.IonValue
import com.amazon.ion.system.IonBinaryWriterBuilder
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ion.system.IonWriterBuilder
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.ion.IonGenerator
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper
import com.fasterxml.jackson.dataformat.ion.IonParser
import com.fasterxml.jackson.dataformat.ion.jsr310.IonJavaTimeModule
import io.grpc.MethodDescriptor
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

class IonMarshaller(
    val ionSystem: IonSystem = IonSystemBuilder.standard().build(),
    val readerBuilder: IonReaderBuilder = IonReaderBuilder.standard(),
    val writerBuilder: IonWriterBuilder = IonBinaryWriterBuilder.standard()
): MethodDescriptor.Marshaller<IonValue> {

    override fun stream(value: IonValue): InputStream {
        return ByteArrayOutputStream().use {
            value.writeTo(writerBuilder.build(it))
            ByteArrayInputStream(it.toByteArray())
        }
    }

    override fun parse(stream: InputStream): IonValue {
        return  ionSystem.loader.load(readerBuilder.build(stream))
    }
}

