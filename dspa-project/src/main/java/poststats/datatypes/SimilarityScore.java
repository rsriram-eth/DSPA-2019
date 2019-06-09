package poststats.datatypes;

import org.apache.flink.api.java.tuple.Tuple7;

public class SimilarityScore implements Comparable<SimilarityScore> {

    // Compare between
    int person1;
    int person2;

    // Features considered
    int currentlyOnline = 0;
    int numOfOnlineInteractions = 0;
    int sameLocation = 0;
    int sameOrganisation = 0;
    int numOfCommonTags = 0;
    int alreadyAFriend = 0;
    int finalSimilarityScore = 0;

    public SimilarityScore(Integer p1, Integer p2, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> t7){

        person1 = p1;
        person2 = p2;
        currentlyOnline = t7.f0;
        numOfOnlineInteractions = t7.f1;
        sameLocation = t7.f2;
        sameOrganisation = t7.f3;
        numOfCommonTags = t7.f4;
        alreadyAFriend = t7.f5;
        finalSimilarityScore = t7.f6;
    }

    @Override
    public int compareTo(SimilarityScore other) {
        return Integer.compare(other.finalSimilarityScore, finalSimilarityScore);
    }

    @Override
    public String toString(){
        return  person1 +" <---> " + person2 +
                " ( " + currentlyOnline + " , "+
                        numOfOnlineInteractions +" , "+
                        sameLocation +" , " +
                        sameOrganisation +" , " +
                        numOfCommonTags + " , " +
                        alreadyAFriend +
                " | Total: " + finalSimilarityScore + " ) ";
    }
}
