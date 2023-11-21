package io.conduit;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CollectionUtilsTest {
    @ParameterizedTest
    @MethodSource("collectionProvider")
    void testIsCollectionEmpty(Collection<?> collection, boolean expected) {
        assertEquals(expected, CollectionUtils.isEmpty(collection));
    }

    static Object[][] collectionProvider() {
        return new Object[][]{
            {null, true},
            {Collections.emptyList(), true},
            {Arrays.asList(1, 2, 3), false},
            {Arrays.asList("a", "b", "c"), false},
            {Collections.singleton("single"), false}
        };
    }

    @ParameterizedTest
    @MethodSource("mapProvider")
    void testIsMapEmpty(Map<?, ?> collection, boolean expected) {
        assertEquals(expected, CollectionUtils.isEmpty(collection));
    }

    static Object[][] mapProvider() {
        return new Object[][]{
            {null, true},
            {Collections.emptyMap(), true},
            {Map.of("a", "b"), false}
        };
    }

}
