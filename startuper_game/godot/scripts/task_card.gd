extends PanelContainer
class_name TaskCard

## Карточка задачи: перетаскивание в слоты спринта (Godot drag & drop).

var task_index: int = -1
var task_data: Dictionary = {}
var main_ref: Control = null
var game_state: GameState = null
## -1 = в бэклоге; 0..2 = слот спринта (важен порядок для базы задач).
var in_slot_index: int = -1

func setup(p_main: Control, p_state: GameState, p_index: int, task: Dictionary) -> void:
	main_ref = p_main
	game_state = p_state
	task_index = p_index
	task_data = task
	in_slot_index = -1
	_rebuild_ui()


func set_slot_index(i: int) -> void:
	in_slot_index = i


func get_slot_index() -> int:
	return in_slot_index


## blocked_slot: задача в слоте не выполнится при текущем порядке; neutral_backlog: карточка в бэклоге.
## synergy: "none" | "pair" | "triple" — подсветка синергии Build/Fit (не перекрывает блок по базе).
func apply_prereq_visual(blocked_slot: bool, neutral_backlog: bool, synergy: String = "none") -> void:
	if neutral_backlog:
		modulate = Color(1, 1, 1, 1)
		return
	if blocked_slot:
		modulate = Color(1.0, 0.82, 0.82, 1.0)
		return
	match synergy:
		"triple":
			modulate = Color(0.78, 0.92, 1.0, 1.0)
		"pair":
			modulate = Color(0.78, 1.0, 0.88, 1.0)
		_:
			modulate = Color(1, 1, 1, 1)

func _rebuild_ui() -> void:
	for c in get_children():
		c.queue_free()

	var vb := VBoxContainer.new()
	vb.add_theme_constant_override("separation", 4)
	add_child(vb)

	var title := Label.new()
	title.text = str(task_data.get("name", ""))
	title.autowrap_mode = TextServer.AUTOWRAP_WORD
	title.add_theme_font_size_override("font_size", 18)
	title.add_theme_color_override("font_color", Color(0.94, 0.96, 0.99))
	title.max_lines_visible = 2
	vb.add_child(title)

	var ill_path: String = str(task_data.get("illustration", "")).strip_edges()
	if ill_path.begins_with("res://") and ResourceLoader.exists(ill_path):
		var txi: Texture2D = load(ill_path) as Texture2D
		if txi != null:
			var ill_rect := TextureRect.new()
			ill_rect.texture = txi
			ill_rect.texture_filter = CanvasItem.TEXTURE_FILTER_LINEAR_WITH_MIPMAPS
			ill_rect.custom_minimum_size = Vector2(220, 72)
			ill_rect.expand_mode = TextureRect.EXPAND_FIT_WIDTH_PROPORTIONAL
			ill_rect.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_CENTERED
			vb.add_child(ill_rect)

	var fx := Label.new()
	if game_state != null:
		fx.text = game_state.describe_task_effects_for_ui(task_data)
	else:
		fx.text = GameState.describe_effects(task_data["effects"])
	fx.autowrap_mode = TextServer.AUTOWRAP_WORD
	fx.add_theme_font_size_override("font_size", 13)
	fx.max_lines_visible = 2
	fx.add_theme_color_override("font_color", Color(0.72, 0.78, 0.88))
	vb.add_child(fx)

	if game_state != null:
		var req_hint := Label.new()
		var hint_text: String = game_state.format_requires_hint(task_data)
		req_hint.text = hint_text if hint_text != "" else "Без порога по базе"
		req_hint.autowrap_mode = TextServer.AUTOWRAP_WORD
		req_hint.add_theme_font_size_override("font_size", 12)
		req_hint.add_theme_color_override("font_color", Color(0.62, 0.7, 0.82))
		req_hint.max_lines_visible = 2
		vb.add_child(req_hint)
		if game_state.task_repeat_chain_bonus_will_apply(task_data):
			var bon := Label.new()
			bon.text = "На этом этапе: +25% к числам (повтор цепочки с прошлого этапа)."
			bon.autowrap_mode = TextServer.AUTOWRAP_WORD
			bon.add_theme_font_size_override("font_size", 11)
			bon.add_theme_color_override("font_color", Color(0.55, 0.92, 0.78))
			bon.max_lines_visible = 2
			vb.add_child(bon)
		if game_state.has_marketer:
			var cmo_note: String = game_state.get_cmo_channel_note_for_task(task_data)
			if cmo_note != "":
				var cm := Label.new()
				cm.text = "CMO: " + cmo_note
				cm.autowrap_mode = TextServer.AUTOWRAP_WORD
				cm.add_theme_font_size_override("font_size", 11)
				cm.add_theme_color_override("font_color", Color(0.72, 0.86, 0.95))
				cm.max_lines_visible = 5
				vb.add_child(cm)

	custom_minimum_size = Vector2(236, 178) if (str(task_data.get("illustration", "")).strip_edges().begins_with("res://")) else Vector2(236, 102)
	mouse_filter = Control.MOUSE_FILTER_STOP

func _get_drag_data(_at_position: Vector2) -> Variant:
	if main_ref == null:
		return null
	if main_ref.has_method("is_planning_locked") and main_ref.is_planning_locked():
		return null

	var from_slot: int = -1
	var p: Node = get_parent()
	while p:
		if p is SprintSlot:
			from_slot = (p as SprintSlot).slot_index
			break
		p = p.get_parent()

	var preview := Label.new()
	preview.text = str(task_data.get("name", ""))
	preview.add_theme_font_size_override("font_size", 12)
	set_drag_preview(preview)

	return {"kind": "task", "task_index": task_index, "from_slot": from_slot}


func _can_drop_data(_at_position: Vector2, data: Variant) -> bool:
	# Важно: когда слот уже занят, курсор попадает в TaskCard, а не в SprintSlot.
	# Проксируем проверку в main, чтобы поддержать replace/swap между занятыми слотами.
	if in_slot_index < 0:
		return false
	if main_ref == null or not main_ref.has_method("can_accept_task_drop"):
		return false
	return main_ref.can_accept_task_drop(data)


func _drop_data(_at_position: Vector2, data: Variant) -> void:
	if in_slot_index < 0:
		return
	if main_ref == null or not main_ref.has_method("on_task_drop_on_slot"):
		return
	main_ref.on_task_drop_on_slot(in_slot_index, data)
