package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import xml.Inventory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;


class MapperUtilTest {

    @Test
    void shouldMapTheXmlFileToJavaObject() throws JsonProcessingException {
        Path xmlFilePath = Paths.get("src", "test", "resources", "sample_xml", "update_inventory.xml");
        String content = null;
        try (Stream<String> lines = Files.lines(xmlFilePath)) {
            content = lines.collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException ignored) {
        }
        Inventory inventory = MapperUtil.mapXmlToInventoryObject(content);
        assertThat(inventory).isNotNull();
        assertThat(inventory.inventoryList()).isNotNull();
        assertThat(inventory.inventoryList().header()).isNotNull();
        assertThat(inventory.inventoryList().records()).isNotNull().isNotEmpty();
        assertThat(inventory.inventoryList().records()).allMatch(record -> nonNull(record.productId()));
    }

}