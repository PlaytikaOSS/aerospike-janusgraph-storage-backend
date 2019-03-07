local function equalBytes(a, b)
    if a == nil and b == nil then
        return true
    else
        if a == nil or b == nil then
            return false
        end
    end

    if bytes.get_string(a, 1, bytes.size(a)) == bytes.get_string(b, 1, bytes.size(b)) then
        return true
    else
        return false
    end
end

local function containsAllEntries(a, b)
    if a == nil and map.size(b) == 0 then
        return true
    end
    for key, value in map.pairs(b) do
        if(equalBytes(a[key], value)) then
        else
            return false
        end
    end
    return true
end

function check_and_lock(rec, lock_ttl, expected_values_map)
    local current_time = aerospike:get_current_time()

    if aerospike:exists(rec) then
        local lock_time = rec['lock_time']

        if not containsAllEntries(rec['entries'], expected_values_map) then
            return 2
        end

        if lock_time and lock_time + lock_ttl > current_time then
            return 1
        end

        rec['lock_time'] = current_time;
        aerospike:update(rec)
        return 0
    else
        rec['lock_time'] = current_time;
        aerospike:create(rec)
        return 0
    end
end