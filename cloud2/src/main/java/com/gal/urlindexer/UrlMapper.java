package com.gal.urlindexer;

import com.google.gson.Gson;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;

/**
 * Created by gbenhaim on 7/7/17.
 */
public class UrlMapper {
    public static final String URL_MAP = "url-map";
    public static final String WORD_MAP = "word-map";

    private final HashMap<String, Double> mUrlMap = new HashMap<String, Double>();
    private final HashMap<String, Set<String>> mWordMap = new HashMap<String, Set<String>>();
    private String mUrl = null;

    public UrlMapper(String url) {
        mUrl = url;
    }

    public void calc() throws IOException {
        Document doc = Jsoup.connect(mUrl).get();
        String words = doc.text();
        Elements links = doc.select("a[href]");

        // add words
        StringTokenizer tokenizer = new StringTokenizer(words, " \t\n\r\f-\\/-.,?!@#$%^&*():;'`~<>|\"");
        while (tokenizer.hasMoreTokens()) {
            addWord(tokenizer.nextToken(), mUrl);
        }

        // add urls
        for (Element link : links) {
            addUrl(link.attr("abs:href"), 1.0);
        }
    }

    public String toJson() {
        HashMap<String, HashMap> map = new HashMap<String, HashMap>();
        map.put(URL_MAP, mUrlMap);
        map.put(WORD_MAP, mWordMap);
        Gson gson = new Gson();
        return gson.toJson(map);
    }

    public String toCSV() {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<String, Set<String>> entry : mWordMap.entrySet()) {
            s.append(String.format("%s, %s", entry.getKey(), entry.getValue()));
            s.append("\n");
        }

        for (Map.Entry<String, Double> entry : mUrlMap.entrySet()) {
            s.append(String.format("%s, %s", entry.getKey(), entry.getValue()));
            s.append("\n");
        }
        s.append("\n");

        return s.toString();
    }

    public void addUrlMap(HashMap<String, Double> urlMap) {
        for (Map.Entry<String, Double> entry : urlMap.entrySet()) {
            addUrl(entry.getKey(), entry.getValue());
        }
    }

    public void addWordMap(HashMap<String, Set<String>> wordMap) {
        for (Map.Entry<String, Set<String>> entry : wordMap.entrySet()) {
            String word = entry.getKey();
            if (! mWordMap.containsKey(word)) {
                mWordMap.put(word, new HashSet<String>());
            }

            mWordMap.get(word).addAll(entry.getValue());
        }
    }

    public HashMap<String, Double> getUrlMap() {
        return mUrlMap;
    }

    public HashMap<String, Set<String>> getWordMap() {
        return mWordMap;
    }

    public String getUrl() {
        return mUrl;
    }

    private void addUrl(String url, Double amount) {
        if (! mUrlMap.containsKey(url)) {
            mUrlMap.put(url, amount);
        } else {
            mUrlMap.put(url, mUrlMap.get(url) + amount);
        }
    }

    private void addWord(String word, String url) {
        String wordLowerCase = word.toLowerCase();

        if (! mWordMap.containsKey(word)) {
            mWordMap.put(wordLowerCase, new HashSet<String>());
        }

        mWordMap.get(wordLowerCase).add(url);
    }



}
