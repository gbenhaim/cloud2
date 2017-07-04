package com.gal.urlindexer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by gbenhaim on 7/4/17.
 */
public class UrlIndexer {

    public static final HashMap<String, Integer> urlMap = new HashMap<String, Integer>();
    public static final HashMap<String, Set<String>> wordMap = new HashMap<String, Set<String>>();

    public static void main(String args[]) throws IOException {
        
        String url = "http://www.ynet.co.il/";
        Document doc = Jsoup.connect(url).get();
        String words = doc.text();
        Elements links = doc.select("a[href]");

        // add words
        StringTokenizer tokenizer = new StringTokenizer(words, " \t\n\r\f-\\/-.,?!@#$%^&*():;'`~<>|");
        while (tokenizer.hasMoreTokens()) {
            addWord(tokenizer.nextToken(), url);
        }

        // add urls
        for (Element link : links) {
            addUrl(link.attr("abs:href"));
        }

        System.out.println(urlMap);
        System.out.println(wordMap);
    }

    public static void addUrl(String url) {
        if (! urlMap.containsKey(url)) {
            urlMap.put(url, 1);
        } else {
            urlMap.put(url, urlMap.get(url) + 1);
        }
    }

    public static void addWord(String word, String url) {
        String wordLowerCase = word.toLowerCase();

        if (! wordMap.containsKey(word)) {
            wordMap.put(wordLowerCase, new HashSet<String>());
        }

        wordMap.get(wordLowerCase).add(url);
    }
}
