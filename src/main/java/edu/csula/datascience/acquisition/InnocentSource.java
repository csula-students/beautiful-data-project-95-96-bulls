package edu.csula.datascience.acquisition;

import java.util.List;
import java.util.Collection;
import com.google.common.collect.Lists;

/**
 * Created by Tazzie on 5/1/2016.
 */
public class InnocentSource implements Source<List<String>>{

    private long minId;
    private final String searchQuery;

    public InnocentSource(long minId, String query) {
        this.minId = minId;
        this.searchQuery = query;
    }

    @Override
    public boolean hasNext() {
        return minId > 0;
    }

    @Override
    public Collection<List<String>> next() {
        List<List<String>> list = Lists.newArrayList();
        return list;
    }

}
