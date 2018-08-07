package com.abyss.mapper;

import java.util.List;

import com.abyss.pojo.TAvgpvNum;
import com.abyss.pojo.TAvgpvNumExample;
import org.apache.ibatis.annotations.Param;

public interface TAvgpvNumMapper {
    int countByExample(TAvgpvNumExample example);

    int deleteByExample(TAvgpvNumExample example);

    int insert(TAvgpvNum record);

    int insertSelective(TAvgpvNum record);

    List<TAvgpvNum> selectByExample(TAvgpvNumExample example);

    int updateByExampleSelective(@Param("record") TAvgpvNum record, @Param("example") TAvgpvNumExample example);

    int updateByExample(@Param("record") TAvgpvNum record, @Param("example") TAvgpvNumExample example);
}