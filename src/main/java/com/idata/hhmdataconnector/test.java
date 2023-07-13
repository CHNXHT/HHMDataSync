package com.idata.hhmdataconnector;
import java.util.List;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.suggest.Suggester;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

public class test {
    public static void main(String[] args) {
        System.out.println(HanLP.segment("逍遥津街道人民调解委员会"));
        System.out.println("\n");

        List<Word> words = WordSegmenter.segWithStopWords("逍遥津街道人民调解委员会");
        System.out.println(words);
    }
}
