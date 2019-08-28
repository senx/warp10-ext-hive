//
//   Copyright 2019  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.script.ext.hive;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ORCTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String ATTR_ORC_TYPEINFO = "orc.typeinfo";
  private static final String ATTR_ORC_TYPEINFO_SPEC = "orc.typeinfo.spec";
  
  public ORCTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
        
    Object top = stack.pop();
    
    TypeInfo typeinfo = null;
    
    if (top instanceof String) {
      // Only generate the typeinfo if the spec is different from the current one
      if (!top.equals(stack.getAttribute(ATTR_ORC_TYPEINFO_SPEC))) {
        typeinfo = TypeInfoUtils.getTypeInfoFromTypeString(top.toString());
  
        stack.setAttribute(ATTR_ORC_TYPEINFO, typeinfo);
        stack.setAttribute(ATTR_ORC_TYPEINFO_SPEC, top.toString());
      } else {
        typeinfo = (TypeInfo) stack.getAttribute(ATTR_ORC_TYPEINFO);        
      }
      top = stack.pop();
    } else if (null == top) {
      // Attempt to infer the typeinfo
      top = stack.pop();
      
      OrcStruct orc = (OrcStruct) top;
      List<StructField> fields = new ArrayList<StructField>(orc.getNumFields());
      for (int i = 0; i < orc.getNumFields(); i++) {
        fields.add(null);
      }
    } else {
      typeinfo = (TypeInfo) stack.getAttribute(ATTR_ORC_TYPEINFO);
    }
    
    if (!(top instanceof OrcStruct)) {      
      throw new WarpScriptException(getName() + " operates on an OrcStruct instance.");
    }

    OrcStruct orc = (OrcStruct) top;
    
//    if (null == typeinfo) {
//      throw new WarpScriptException(getName() + " needs a type info schema on at least the first call.");
//    }
//    
    ObjectInspector inspector = null != typeinfo ? OrcStruct.createObjectInspector(typeinfo) : null;

    stack.push(conv(orc, inspector));
    
    return stack;
  }
  
  public static Object conv(Object o, ObjectInspector inspector) throws WarpScriptException {
    
    // Attempt to infer an inspector if none was provided
    if (null == inspector) {
      if (o instanceof OrcStruct) {
        OrcStruct orc = (OrcStruct) o;
      }
    }

    if (null == o) {
      return o;
    }
    
    switch(inspector.getCategory()) {
      case LIST:
        ListObjectInspector loi = (ListObjectInspector) inspector;
        ObjectInspector eltInspector = loi.getListElementObjectInspector();
        
        List<Object> list = new ArrayList<Object>(loi.getListLength(o));
        
        for (Object elt: loi.getList(o)) {
          list.add(conv(elt, eltInspector));
        }        
        return list;
        
      case MAP:
        MapObjectInspector moi = (MapObjectInspector) inspector;
        ObjectInspector keyInspector = moi.getMapKeyObjectInspector();
        ObjectInspector valueInspector = moi.getMapValueObjectInspector();
        
        Map<Object,Object> map = new HashMap<Object,Object>(moi.getMapSize(o));
        
        for (Entry entry: moi.getMap(o).entrySet()) {
          map.put(conv(entry.getKey(), keyInspector), conv(entry.getValue(), valueInspector));
        }
        
        return map;

      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
        
        Object prim = poi.getPrimitiveJavaObject(o);
        
        if (null == prim) {
          return null;
        }
        
        switch (poi.getPrimitiveCategory()) {
          case BINARY:
          case BOOLEAN:
          case DOUBLE:
          case LONG:
          case STRING:
          case VARCHAR:
            return prim;
            
          case CHAR:
            return Character.toString((char) prim);
            
          case BYTE:
          case SHORT:
          case INT:
            return ((Number) prim).longValue();            
            
          case FLOAT:
            return ((Number) prim).doubleValue();

          case DECIMAL:
            return ((HiveDecimal) prim).doubleValue();
            
          case TIMESTAMPLOCALTZ:
          case TIMESTAMP:
            long ts = (1000000000L * (((Timestamp) prim).getTime() / 1000L) + ((Timestamp) prim).getNanos()) / Constants.NS_PER_TIME_UNIT;
            return ts;

          case DATE:
            long dts = ((Date) prim).getTime() * Constants.TIME_UNITS_PER_MS;
            return dts;
            
          case INTERVAL_DAY_TIME:
            long idt = ((((HiveIntervalDayTime) prim).getTotalSeconds() * 1000000000L) + ((HiveIntervalDayTime) prim).getNanos()) / Constants.NS_PER_TIME_UNIT;
            return idt;
            
          case INTERVAL_YEAR_MONTH:
            return ((HiveIntervalYearMonth) prim).getTotalMonths();
            
          case UNKNOWN:
          case VOID:
            throw new WarpScriptException("Yet unsupported primitive type " + prim.getClass());
        }

      case STRUCT:
        StructObjectInspector soi = (StructObjectInspector) inspector;
        List<StructField> fields = (List<StructField>) soi.getAllStructFieldRefs();
        
        Map<String,Object> struct = new HashMap<String,Object>();
        
        for (StructField field: fields) {
          struct.put(field.getFieldName(), conv(soi.getStructFieldData(o, field), field.getFieldObjectInspector()));
        }
        
        return struct;

      case UNION:
        UnionObjectInspector uoi = (UnionObjectInspector) inspector;
        ObjectInspector insp = uoi.getObjectInspectors().get(uoi.getTag(o));
        return conv(uoi.getField(o), insp);
        
      default:
        return null;
    }  
  }
}
