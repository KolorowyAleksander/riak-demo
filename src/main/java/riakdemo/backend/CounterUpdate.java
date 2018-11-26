package riakdemo.backend;

import com.basho.riak.client.api.commands.kv.UpdateValue;

public class CounterUpdate extends UpdateValue.Update<Integer> {

    private Integer update;

    @Override
    public Integer apply(Integer original) {
        if (original == null) {
            original = new Integer(0);
        }

        original = original + 1;
        return original;
    }
}


