extends PanelContainer
class_name SprintSlot

## Слот спринта: принимает карточку задачи.

@export var slot_index: int = 0
var main_ref: Control = null

func _ready() -> void:
	mouse_filter = Control.MOUSE_FILTER_STOP
	custom_minimum_size = Vector2(148, 96)
	_refresh_empty_look()

func set_main(p_main: Control) -> void:
	main_ref = p_main

func _can_drop_data(_at_position: Vector2, data: Variant) -> bool:
	if main_ref == null or not main_ref.has_method("can_accept_task_drop"):
		return false
	return main_ref.can_accept_task_drop(data)

func _drop_data(_at_position: Vector2, data: Variant) -> void:
	if main_ref == null or not main_ref.has_method("on_task_drop_on_slot"):
		return
	main_ref.on_task_drop_on_slot(slot_index, data)

func mount_card(card: TaskCard) -> void:
	for ch in get_children():
		ch.queue_free()
	card.set_slot_index(slot_index)
	add_child(card)

func show_empty() -> void:
	_refresh_empty_look()

func _refresh_empty_look() -> void:
	for c in get_children():
		c.queue_free()
	var lbl := Label.new()
	lbl.text = "Слот %d\n↓ перетащи задачу" % (slot_index + 1)
	lbl.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	lbl.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	lbl.add_theme_font_size_override("font_size", 12)
	lbl.add_theme_color_override("font_color", Color(0.55, 0.62, 0.72))
	lbl.set_anchors_preset(Control.PRESET_FULL_RECT)
	add_child(lbl)
