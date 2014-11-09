package org.jmmo.sc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QuotesTest {

    @Test
    public void testAddQuotes() throws Exception {
        assertEquals("\"\"", Quotes.addQuotes(""));
        assertEquals("\"\"", Quotes.addQuotes("\""));
        assertEquals("\"Users\"", Quotes.addQuotes("Users"));
        assertEquals("\"Users\"", Quotes.addQuotes("\"Users"));
        assertEquals("\"Users\"", Quotes.addQuotes("Users\""));
    }

    @Test
    public void testRemoveQuotes() throws Exception {
        assertEquals("", Quotes.removeQuotes(""));
        assertEquals("", Quotes.removeQuotes("\"\""));
        assertEquals("without", Quotes.removeQuotes("without"));
        assertEquals("Users", Quotes.removeQuotes("\"Users\""));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveQuotesSingleQuoteFirst() throws Exception {
        Quotes.removeQuotes("\"Users");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveQuotesSingleQuoteLast() throws Exception {
        Quotes.removeQuotes("Users\"");
    }
}